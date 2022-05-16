#pragma once

#include <type_traits>
#include <base/types.h>
#include <Common/Volnitsky.h>
#include <Columns/ColumnString.h>
#include "Regexps.h"

#include "config_functions.h"
#include <Common/config.h>
#include <re2_st/re2.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


/// Is the [I]LIKE expression reduced to finding a substring in a string?
static inline bool likePatternIsStrstr(const String & pattern, String & res)
{
    if (pattern.size() < 2 || pattern.front() != '%' || pattern.back() != '%')
        return false;

    res = "";
    res.reserve(pattern.size() - 2);

    const char * pos = pattern.data();
    const char * end = pos + pattern.size();

    ++pos;
    --end;

    while (pos < end)
    {
        switch (*pos)
        {
            case '%':
            case '_':
                return false;
            case '\\':
                ++pos;
                if (pos == end)
                    return false;
                else
                    res += *pos;
                break;
            default:
                res += *pos;
                break;
        }
        ++pos;
    }

    return true;
}

/** 'like'             - if true, treat pattern as SQL LIKE, otherwise as re2 regexp.
 *  'negate'           - if true, negate result
 *  'case_insensitive' - if true, match case insensitively
 *
  * NOTE: We want to run regexp search for whole columns by one call (as implemented in function 'position')
  *  but for that, regexp engine must support \0 bytes and their interpretation as string boundaries.
  */
template <typename Name, bool like, bool negate, bool case_insensitive>
struct MatchImpl
{
    static constexpr bool use_default_implementation_for_constants = true;
    static constexpr bool supports_start_pos = false;
    static constexpr auto name = Name::name;

    using ResultType = UInt8;

    using Searcher = std::conditional_t<case_insensitive,
          VolnitskyCaseInsensitiveUTF8,
          VolnitskyUTF8>;

    static void vectorConstant(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const String & pattern,
        const ColumnPtr & start_pos,
        PaddedPODArray<UInt8> & res)
    {
        if (start_pos != nullptr)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Function '{}' doesn't support start_pos argument", name);

        if (haystack_offsets.empty())
            return;

        /// A simple case where the [I]LIKE expression reduces to finding a substring in a string
        String strstr_pattern;
        if (like && likePatternIsStrstr(pattern, strstr_pattern))
        {
            const UInt8 * const begin = haystack_data.data();
            const UInt8 * const end = haystack_data.data() + haystack_data.size();
            const UInt8 * pos = begin;

            /// The current index in the array of strings.
            size_t i = 0;

            /// TODO You need to make that `searcher` is common to all the calls of the function.
            Searcher searcher(strstr_pattern.data(), strstr_pattern.size(), end - pos);

            /// We will search for the next occurrence in all rows at once.
            while (pos < end && end != (pos = searcher.search(pos, end - pos)))
            {
                /// Let's determine which index it refers to.
                while (begin + haystack_offsets[i] <= pos)
                {
                    res[i] = negate;
                    ++i;
                }

                /// We check that the entry does not pass through the boundaries of strings.
                if (pos + strstr_pattern.size() < begin + haystack_offsets[i])
                    res[i] = !negate;
                else
                    res[i] = negate;

                pos = begin + haystack_offsets[i];
                ++i;
            }

            /// Tail, in which there can be no substring.
            if (i < res.size())
                memset(&res[i], negate, (res.size() - i) * sizeof(res[0]));
        }
        else
        {
            auto regexp = Regexps::get<like, true, case_insensitive>(pattern);

            String required_substring;
            bool is_trivial;
            bool required_substring_is_prefix; /// for `anchored` execution of the regexp.

            regexp->getAnalyzeResult(required_substring, is_trivial, required_substring_is_prefix);

            size_t haystack_size = haystack_offsets.size();

            if (required_substring.empty())
            {
                if (!regexp->getRE2()) /// An empty regexp. Always matches.
                {
                    if (haystack_size)
                        memset(res.data(), 1, haystack_size * sizeof(res[0]));
                }
                else
                {
                    size_t prev_offset = 0;
                    for (size_t i = 0; i < haystack_size; ++i)
                    {
                        res[i] = negate
                            ^ regexp->getRE2()->Match(
                                  Regexps::Regexp::StringPieceType(reinterpret_cast<const char *>(&haystack_data[prev_offset]), haystack_offsets[i] - prev_offset - 1),
                                  0,
                                  haystack_offsets[i] - prev_offset - 1,
                                  re2_st::RE2::UNANCHORED,
                                  nullptr,
                                  0);

                        prev_offset = haystack_offsets[i];
                    }
                }
            }
            else
            {
                /// NOTE This almost matches with the case of LikePatternIsStrstr.

                const UInt8 * const begin = haystack_data.data();
                const UInt8 * const end = haystack_data.begin() + haystack_data.size();
                const UInt8 * pos = begin;

                /// The current index in the array of strings.
                size_t i = 0;

                Searcher searcher(required_substring.data(), required_substring.size(), end - pos);

                /// We will search for the next occurrence in all rows at once.
                while (pos < end && end != (pos = searcher.search(pos, end - pos)))
                {
                    /// Determine which index it refers to.
                    while (begin + haystack_offsets[i] <= pos)
                    {
                        res[i] = negate;
                        ++i;
                    }

                    /// We check that the entry does not pass through the boundaries of strings.
                    if (pos + required_substring.size() < begin + haystack_offsets[i])
                    {
                        /// And if it does not, if necessary, we check the regexp.

                        if (is_trivial)
                            res[i] = !negate;
                        else
                        {
                            const char * str_data = reinterpret_cast<const char *>(&haystack_data[haystack_offsets[i - 1]]);
                            size_t str_size = haystack_offsets[i] - haystack_offsets[i - 1] - 1;

                            /** Even in the case of `required_substring_is_prefix` use UNANCHORED check for regexp,
                              *  so that it can match when `required_substring` occurs into the string several times,
                              *  and at the first occurrence, the regexp is not a match.
                              */

                            if (required_substring_is_prefix)
                                res[i] = negate
                                    ^ regexp->getRE2()->Match(
                                          Regexps::Regexp::StringPieceType(str_data, str_size),
                                          reinterpret_cast<const char *>(pos) - str_data,
                                          str_size,
                                          re2_st::RE2::UNANCHORED,
                                          nullptr,
                                          0);
                            else
                                res[i] = negate
                                    ^ regexp->getRE2()->Match(
                                          Regexps::Regexp::StringPieceType(str_data, str_size), 0, str_size, re2_st::RE2::UNANCHORED, nullptr, 0);
                        }
                    }
                    else
                        res[i] = negate;

                    pos = begin + haystack_offsets[i];
                    ++i;
                }

                /// Tail, in which there can be no substring.
                if (i < res.size())
                    memset(&res[i], negate, (res.size() - i) * sizeof(res[0]));
            }
        }
    }

    /// Very carefully crafted copy-paste.
    static void vectorFixedConstant(
        const ColumnString::Chars & data,
        size_t n,
        const String & pattern,
        PaddedPODArray<UInt8> & res)
    {
        if (data.empty())
            return;

        /// A simple case where the LIKE expression reduces to finding a substring in a string
        String strstr_pattern;
        if (like && likePatternIsStrstr(pattern, strstr_pattern))
        {
            const UInt8 * const begin = data.data();
            const UInt8 * const end = data.data() + data.size();
            const UInt8 * pos = begin;

            size_t i = 0;
            const UInt8 * next_pos = begin;

            /// If pattern is larger than string size - it cannot be found.
            if (strstr_pattern.size() <= n)
            {
                Searcher searcher(strstr_pattern.data(), strstr_pattern.size(), end - pos);

                /// We will search for the next occurrence in all rows at once.
                while (pos < end && end != (pos = searcher.search(pos, end - pos)))
                {
                    /// Let's determine which index it refers to.
                    while (next_pos + n <= pos)
                    {
                        res[i] = negate;
                        next_pos += n;
                        ++i;
                    }
                    next_pos += n;

                    /// We check that the entry does not pass through the boundaries of strings.
                    if (pos + strstr_pattern.size() <= next_pos)
                        res[i] = !negate;
                    else
                        res[i] = negate;

                    pos = next_pos;
                    ++i;
                }
            }

            /// Tail, in which there can be no substring.
            if (i < res.size())
                memset(&res[i], negate, (res.size() - i) * sizeof(res[0]));
        }
        else
        {
            auto regexp = Regexps::get<like, true, case_insensitive>(pattern);

            String required_substring;
            bool is_trivial;
            bool required_substring_is_prefix; /// for `anchored` execution of the regexp.

            regexp->getAnalyzeResult(required_substring, is_trivial, required_substring_is_prefix);

            size_t size = data.size() / n;

            if (required_substring.empty())
            {
                if (!regexp->getRE2()) /// An empty regexp. Always matches.
                {
                    if (size)
                        memset(res.data(), 1, size * sizeof(res[0]));
                }
                else
                {
                    size_t offset = 0;
                    for (size_t i = 0; i < size; ++i)
                    {
                        res[i] = negate
                            ^ regexp->getRE2()->Match(
                                  Regexps::Regexp::StringPieceType(reinterpret_cast<const char *>(&data[offset]), n),
                                  0,
                                  n,
                                  re2_st::RE2::UNANCHORED,
                                  nullptr,
                                  0);

                        offset += n;
                    }
                }
            }
            else
            {
                /// NOTE This almost matches with the case of LikePatternIsStrstr.

                const UInt8 * const begin = data.data();
                const UInt8 * const end = data.data() + data.size();
                const UInt8 * pos = begin;

                size_t i = 0;
                const UInt8 * next_pos = begin;

                /// If required substring is larger than string size - it cannot be found.
                if (required_substring.size() <= n)
                {
                    Searcher searcher(required_substring.data(), required_substring.size(), end - pos);

                    /// We will search for the next occurrence in all rows at once.
                    while (pos < end && end != (pos = searcher.search(pos, end - pos)))
                    {
                        /// Let's determine which index it refers to.
                        while (next_pos + n <= pos)
                        {
                            res[i] = negate;
                            next_pos += n;
                            ++i;
                        }
                        next_pos += n;

                        if (pos + required_substring.size() <= next_pos)
                        {
                            /// And if it does not, if necessary, we check the regexp.

                            if (is_trivial)
                                res[i] = !negate;
                            else
                            {
                                const char * str_data = reinterpret_cast<const char *>(next_pos - n);

                                /** Even in the case of `required_substring_is_prefix` use UNANCHORED check for regexp,
                                *  so that it can match when `required_substring` occurs into the string several times,
                                *  and at the first occurrence, the regexp is not a match.
                                */

                                if (required_substring_is_prefix)
                                    res[i] = negate
                                        ^ regexp->getRE2()->Match(
                                            Regexps::Regexp::StringPieceType(str_data, n),
                                            reinterpret_cast<const char *>(pos) - str_data,
                                            n,
                                            re2_st::RE2::UNANCHORED,
                                            nullptr,
                                            0);
                                else
                                    res[i] = negate
                                        ^ regexp->getRE2()->Match(
                                            Regexps::Regexp::StringPieceType(str_data, n), 0, n, re2_st::RE2::UNANCHORED, nullptr, 0);
                            }
                        }
                        else
                            res[i] = negate;

                        pos = next_pos;
                        ++i;
                    }
                }

                /// Tail, in which there can be no substring.
                if (i < res.size())
                    memset(&res[i], negate, (res.size() - i) * sizeof(res[0]));
            }
        }
    }

    static void vectorVector(
        const ColumnString::Chars & haystack_data,
        const ColumnString::Offsets & haystack_offsets,
        const ColumnString::Chars & needle_data,
        const ColumnString::Offsets & needle_offset,
        const ColumnPtr & start_pos,
        PaddedPODArray<UInt8> & res)
    {
        if (haystack_offsets.size() != needle_offset.size())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Function '{}' unexpectedly received a different number of haystacks and needles", name);

        if (start_pos != nullptr)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Function '{}' doesn't support start_pos argument", name);

        if (haystack_offsets.empty())
            return;

        size_t haystack_size = haystack_offsets.size();

        String required_substr;
        bool is_trivial;
        bool required_substring_is_prefix; /// for `anchored` execution of the regexp.

        size_t prev_haystack_offset = 0;
        size_t prev_needle_offset = 0;

        for (size_t i = 0; i < haystack_size; ++i)
        {
            const auto * const cur_haystack_data = &haystack_data[prev_haystack_offset];
            const size_t cur_haystack_offset = haystack_offsets[i] - prev_haystack_offset - 1;

            const auto * const cur_needle_data = &needle_data[prev_needle_offset];
            const size_t cur_needle_offset = needle_offset[i] - prev_needle_offset - 1;

            const auto & needle = String(
                    reinterpret_cast<const char *>(cur_needle_data),
                    cur_needle_offset);

            if (like && likePatternIsStrstr(needle, required_substr))
            {
                const auto * const required_substr_data = required_substr.data();
                const size_t required_substr_size = required_substr.size();

                Searcher searcher(required_substr_data, required_substr_size, cur_haystack_offset - prev_haystack_offset);
                const auto * match = searcher.search(cur_haystack_data, cur_haystack_offset);
                res[i] = negate
                    ^ (match != cur_haystack_data + cur_haystack_offset);
            }
            else
            {
                // each row is expected to contain a different like/re2 pattern
                // --> bypass the regexp cache, instead construct the pattern on-the-fly
                int flags = Regexps::Regexp::RE_DOT_NL;
                flags |= Regexps::Regexp::RE_NO_CAPTURE;
                if (case_insensitive)
                    flags |= Regexps::Regexp::RE_CASELESS;

                const auto & regexp = Regexps::Regexp(Regexps::createRegexp<like>(needle, flags));

                regexp.getAnalyzeResult(required_substr, is_trivial, required_substring_is_prefix);

                res[i] = negate
                    ^ regexp.getRE2()->Match(
                                  Regexps::Regexp::StringPieceType(reinterpret_cast<const char *>(cur_haystack_data), cur_haystack_offset),
                                  0,
                                  haystack_offsets[i] - prev_haystack_offset - 1,
                                  re2_st::RE2::UNANCHORED,
                                  nullptr,
                                  0);
            }

            prev_haystack_offset = haystack_offsets[i];
            prev_needle_offset = needle_offset[i];
        }
    }

    template <typename... Args>
    static void constantVector(Args &&...)
    {
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Function '{}' doesn't support search with non-constant needles in constant haystack", name);
    }
};

}
