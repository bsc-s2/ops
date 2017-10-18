#!/usr/bin/env python2
# coding: utf-8

import unittest

from pykit import strutil
from pykit import ututil

dd = ututil.dd


def dd_lines(lines):
    dd()
    for l in lines:
        dd(l)


class TestStrutil(unittest.TestCase):

    def test_tokenize(self):

        base_cases = (
                ('', [''], ''),
                ('a', ['a'], ''),
                ('a b', ['a', 'b'], ''),
                (' a', ['', 'a'], ''),
                ('b ', ['b', ''], ''),
                ('abc def gh', ['abc', 'def', 'gh'], ''),
                ('"ab cd"', ['"ab cd"'], ''),
                ('"ab  cd"', ['"ab  cd"'], 'multiple space inside quotes'),
                ('"ab cd" ', ['"ab cd"', ''], ''),
                (' "ab cd"', ['', '"ab cd"'], ''),
                ('"ab cd" "x', ['"ab cd"'], 'discard incomplete'),
                ('"ab cd" "x y"', ['"ab cd"', '"x y"'], ''),

                ('foo "ab cd" "x y"', ['foo', '"ab cd"', '"x y"'], ''),
                ('foo "ab cd" "x', ['foo', '"ab cd"'], 'discard incomplete'),

                ('foo "\\"ab cd" "x', ['foo', '""ab cd"'], 'escape "'),
                ('foo "\\\\"ab cd" "x', ['foo', '"\\"ab', 'cd" "x'], 'escape \\'),
                ('foo "\\\\\\"ab cd" "x', ['foo', '"\\"ab cd"'], 'escape \\ "'),

                ('a \\"bc "d e" "f',  ['a', '"bc', '"d e"'], ''),
                ('a \\\\"bc "d e" "f',  ['a', '\\"bc "d', 'e" "f'], ''),
                ('a \\\\\\"bc "d e" "f',  ['a', '\\"bc', '"d e"'], ''),

                ('a "bc "d \\"f',  ['a', '"bc "d', '"f'], ''),
                ('a "bc "d \\\\"f',  ['a', '"bc "d'], ''),
                ('a "bc "d \\\\\\"f',  ['a', '"bc "d', '\\"f'], ''),

                ('\\"bc "d "f',  ['"bc', '"d "f'], ''),
                ('\\\\"bc "d "f',  ['\\"bc "d'], ''),
                ('\\\\\\"bc "d "f',  ['\\"bc', '"d "f'], ''),

                ('a "bc "d f\\"',  ['a', '"bc "d', 'f"'], ''),
                ('a "bc "d f\\\\"',  ['a', '"bc "d'], ''),
                ('a "bc "d f\\\\\\"',  ['a', '"bc "d', 'f\\"'], ''),
        )

        for _in, _out, _mes in base_cases:
            rst = strutil.tokenize(_in, sep=' ', preserve=True)
            self.assertEqual(_out, rst,
                             ('input: {_in}, output: {_out}, expected: {rst},'
                              ' message: {_mes}').format(
                                 _in=repr(_in),
                                 _out=repr(_out),
                                 rst=repr(rst),
                                 _mes=_mes
                             ))

        sep_cases = (
                ('',              None,    True,   []),
                (' a  b  c ',     None,    True,   ['a', 'b', 'c']),
                (' a  "b  c" ',   None,    True,   ['a', '"b  c"']),
                (' a  "b  c" ',   None,    False,  ['a', 'b  c']),
                ('a b c',         None,    True,   ['a', 'b', 'c']),
                ('"a b c"',       None,    True,   ['"a b c"']),
                ('"a b c"',       None,    False,  ['a b c']),
                ('a b"c d"',      None,    True,   ['a', 'b"c d"']),
                ('a b"c d"',      None,    False,  ['a', 'bc d']),
                ('a bcd',         'bc',    True,   ['a ', 'd']),
                ('a "bc" d',      'bc',    True,   ['a "bc" d']),
                ('a "bc" d',      'bc',    False,  ['a bc d']),
                ('abcd',          'abcd',  True,   ['', '']),
        )

        for line, sep, preserve, rst_expected in sep_cases:
            dd('in: ', line, sep)
            rst = strutil.tokenize(line, sep=sep, quote='"', preserve=preserve)
            dd('out: ', rst)
            self.assertEqual(rst, rst_expected)

        preserve_cases = (
                ('""',                   '"',    True,    ['""']),
                ('""',                   '"',    False,   ['']),
                ('abc xd efx gh',        'x',    True,    ['abc', 'xd efx', 'gh']),
                ('abc xd efx gh',        'x',    False,   ['abc', 'd ef', 'gh']),
                ('ab cxd efx gh',        'x',    True,    ['ab', 'cxd efx', 'gh']),
                ('ab cxd efx gh',        'x',    False,   ['ab', 'cd ef', 'gh']),
                ('ab cxd efxgh',         'x',    True,    ['ab', 'cxd efxgh']),
                ('ab cxd efxgh',         'x',    False,   ['ab', 'cd efgh']),
                ('ab cxd yey fx gh',     'xy',   True,    ['ab', 'cxd yey fx', 'gh']),
                ('ab cxd yey fx gh',     'xy',   False,   ['ab', 'cd yey f', 'gh']),
                ('ab cxd yey f gh',      'xy',   True,    ['ab']),
                ('ab cxd yey f gh',      'xy',   False,   ['ab']),
                ('ab cxd xex f gh',      'x',    True,    ['ab']),
                ('ab cxd xex f gh',      'x',    False,   ['ab']),
        )

        for line, quote, preserve, rst_expected in preserve_cases:
            dd('in: ', line, quote, preserve)
            rst = strutil.tokenize(line, sep=' ', quote=quote, preserve=preserve)
            dd('out: ', rst)
            self.assertEqual(rst, rst_expected)

    def test_line_pad(self):

        cases = (
                (('', ''),
                 '',
                 ''
                 ),
                (('a', ''),
                 'a',
                 ''
                 ),
                (('a', ' '),
                 ' a',
                 ''
                 ),
                (('a', '   '),
                 '   a',
                 ''
                 ),
                (('a\nb', '   '),
                 '   a\n'
                 '   b',
                 ''
                 ),
                (('a\nbc', lambda line: 'x' * len(line)),
                 'xa\n'
                 'xxbc',
                 ''
                 ),
        )

        for _in, _out, _mes in cases:
            rst = strutil.line_pad(*_in)
            self.assertEqual(_out, rst,
                             ('input: {_in}, output: {_out}, expected: {rst},'
                              ' message: {_mes}').format(
                                 _in=repr(_in),
                                 _out=repr(_out),
                                 rst=repr(rst),
                                 _mes=_mes
                             ))

    def test_format_line(self):

        cases = (
                (([], '', ''),
                 '',
                 ''
                 ),
                ((['a'], '', ''),
                 'a',
                 ''
                 ),
                (([['a', 'bc']], '', ''),
                 ' a\n'
                 'bc',
                 'default alignment is to right'
                 ),
                (([['a', 'bc'], 'foo'], '', ''),
                 ' afoo\n'
                 'bc   ',
                 'no sep, add space to align'
                 ),
                (([['a', 'bc'], 'foo'], '|', ''),
                 ' a|foo\n'
                 'bc|   ',
                 'sep is "|"'
                 ),
                (([['a', 'bc'], 'foo', [1, 2, 333]], '|', ''),
                 ' a|foo|  1\n'
                 'bc|   |  2\n'
                 '  |   |333',
                 'number will be convert to str'
                 ),
                (([['a', 'bc'], 'foo', [1, 2, strutil.blue('xp')]], '|', ''),
                 ' a|foo| 1\n'
                 'bc|   | 2\n'
                 '  |   |' + str(strutil.blue('xp')),
                 'strutil.ColoredString instance is compatible'
                 ),
        )

        for _in, _out, _mes in cases:

            rst = strutil.format_line(*_in)

            dd("_in: " + str(_in))
            dd("rst:\n" + rst)

            self.assertEqual(_out, rst,
                             ('input: {_in}, output: {_out}, expected: {rst},'
                              ' message: {_mes}').format(
                                 _in=repr(_in),
                                 _out=repr(_out),
                                 rst=repr(rst),
                                 _mes=_mes
                             ))

    def test_struct_repr(self):

        inp = {
            1: 3,
            'x': {1: 4, 2: 5},
            'yyy': [1, 2, 3, 1000],
        }

        rst = strutil.struct_repr(inp)

        for l in rst:
            dd(repr(l))

        expected = [
            '  1 : 3     ',
            '  x : 1 : 4 ',
            '      2 : 5 ',
            'yyy : - 1   ',
            '      - 2   ',
            '      - 3   ',
            '      - 1000',
        ]

        self.assertEqual(expected, rst)

    def test_format_table(self):

        inp = [
            {'acl': {},
             'bucket': 'game1.read',
             'bucket_id': '1400000000000689036',
             'num_used': '0',
             'owner': 'game1',
             'space_used': '0',
             'ts': '1492091893065708032'},
            {'acl': {},
             'bucket': 'game2.read',
             'bucket_id': '1510000000000689037',
             'num_used': '0',
             'owner': 'game2',
             'space_used': '0',
             'ts': '1492091906629786880'},
            {'acl': {'imgx': ['READ', 'READ_ACP', 'WRITE', 'WRITE_ACP']},
             'bucket': 'imgx-test',
             'bucket_id': '1910000000000689048',
             'num_used': '0',
             'owner': 'imgx',
             'space_used': '0',
             'ts': '1492101189213795840'}]

        # default
        rst = strutil.format_table(inp)
        dd_lines(rst)

        # the last line is a '\n' splitted multi-row line
        expected = [
            'acl:               | bucket:    | bucket_id:          | num_used:  | owner:  | space_used:  | ts:                ',
            '{}                 | game1.read | 1400000000000689036 | 0          | game1   | 0            | 1492091893065708032',
            '{}                 | game2.read | 1510000000000689037 | 0          | game2   | 0            | 1492091906629786880',
            'imgx : - READ      | imgx-test  | 1910000000000689048 | 0          | imgx    | 0            | 1492101189213795840\n'
            '       - READ_ACP  |            |                     |            |         |              |                    \n'
            '       - WRITE     |            |                     |            |         |              |                    \n'
            '       - WRITE_ACP |            |                     |            |         |              |                    ',
        ]
        self.assertEqual(expected, rst)

        # specify key to render

        rst = strutil.format_table(inp, keys=[['bucket', 'B']])
        dd_lines(rst)
        expected = [
            'B:        ',
            'game1.read',
            'game2.read',
            'imgx-test ',
        ]
        self.assertEqual(expected, rst)

        # row_sep
        rst = strutil.format_table(inp, keys=['bucket'], row_sep='+')
        dd_lines(rst)
        expected = [
            'bucket:   ',
            '++++++++++',
            'game1.read',
            '++++++++++',
            'game2.read',
            '++++++++++',
            'imgx-test ',
        ]
        self.assertEqual(expected, rst)

        # sep
        rst = strutil.format_table(
            inp, keys=['bucket', 'bucket_id'], sep=' # ')
        dd_lines(rst)
        expected = [
            'bucket:    # bucket_id:         ',
            'game1.read # 1400000000000689036',
            'game2.read # 1510000000000689037',
            'imgx-test  # 1910000000000689048',
        ]
        self.assertEqual(expected, rst)

    def test_utf8str(self):
        cases = (
                ('', ''),
                ('1', '1'),
                (1, '1'),
                (u'我', '\xe6\x88\x91'),
                ('\xe6\x88\x91', '\xe6\x88\x91'),
        )

        for inp, expected in cases:

            rst = strutil.utf8str(inp)

            self.assertEqual(expected, rst)

    def test_break_line(self):
        case = [
            ('a quick brown fox jumps over the lazy dog',        13,    ['a quick brown', 'fox jumps', 'over the lazy', 'dog']),
            ('a quick\nbrown fox jumps over the lazy dog',       13,    ['a quick', 'brown fox', 'jumps over', 'the lazy dog']),
            ('a quick\rbrown fox jumps over the lazy dog',       13,    ['a quick', 'brown fox', 'jumps over', 'the lazy dog']),
            ('a quick brown fox jumps\r\nover the lazy dog',     13,    ['a quick brown', 'fox jumps', 'over the lazy', 'dog']),
            ('a quick\nbrown\rfox jumps\r\nover the lazy dog',   13,    ['a quick', 'brown', 'fox jumps', 'over the lazy', 'dog']),
            ('aquickbrownfoxjumpsoverthelazydog',                9,     ['aquickbrownfoxjumpsoverthelazydog']),
            ('aquickbro',                                        9,     ['aquickbro']),
            (' aquickbro',                                       9,     ['', 'aquickbro']),
            ('  aquickbro',                                      9,     [' ', 'aquickbro']),
            ('aquickbro ',                                       9,     ['aquickbro']),
            ('aquickbro  ',                                      9,     ['aquickbro', ' ']),
            ('aqu ick br',                                       9,     ['aqu ick', 'br']),
            ('aqu ick br',                                       9.34,  ['aqu ick', 'br']),
            ('apu   ick  br',                                    5,     ['apu  ', 'ick ', 'br']),
            ('aqu ick br',                                       0,     ['aqu', 'ick', 'br']),
            ('aqu ick br',                                       -1,    ['aqu', 'ick', 'br']),
            ('',                                                 2,     []),
            (' ',                                                2,     [' ']),
            ('  ',                                               2,     ['  ']),
            ('   ',                                              2,     ['  ']),
            ('    ',                                             2,     ['  ', ' ']),
        ]

        for linestr, width, expected in case:
            rst = strutil.break_line(linestr, width)
            dd('case: ', linestr, width, expected)
            dd('rst: ', rst)
            self.assertEqual(rst, expected)


class TestColoredString(unittest.TestCase):

    cs = strutil.ColoredString

    def test_colorize_input(self):

        cases = (
                (0, 0),

                (0, 1),
                (0, 100),
                (1, 100),
                (50, 100),
                (100, 100),

                (0, -1),
                (0, -100),
                (1, -100),
                (50, -100),
                (100, -100),
        )

        print

        for v, total in cases:
            print strutil.colorize(v, total, str(v) + '/' + str(total))

    def test_show_all_colors(self):

        for c in range(256):
            if c % 16 == 0:
                print
            print self.cs('{0:>3}'.format(c), c),

    def test_named_color(self):

        print

        # named color shortcuts
        print strutil.blue('blue'),
        print strutil.cyan('cyan'),
        print strutil.green('green'),
        print strutil.yellow('yellow'),
        print strutil.red('red'),
        print strutil.purple('purple'),
        print strutil.white('white'),
        print

        print strutil.danger('danger'),
        print strutil.warn('warn'),
        print strutil.loaded('loaded'),
        print strutil.normal('normal'),
        print strutil.optimal('optimal'),
        print

    def test_pading(self):

        print
        for i in range(0, 100, 5):
            print strutil.colorize(i, 100),
        print

        for i in range(0, 100, 5):
            print strutil.colorize(i, -100),
        print

    def test_length(self):

        cases = (
                '',
                'string',
                '幾時何時',
                '\xf3',
        )

        for v in cases:
            self.assertEqual(len(self.cs(v, 'warn')), len(v))

    def test_add(self):

        # concat colored string with '+', like normal string
        s = self.cs('danger', 'danger') + self.cs('warn', 'warn')
        self.assertEqual(len('danger' + 'warn'), len(s))

        s += 'extra_string'
        self.assertEqual(len('danger' + 'warn' + 'extra_string'), len(s))

    def test_mul(self):

        # colored string can be duplicated with '*', like normal string
        s = (self.cs('danger', 'danger')
             + self.cs('warn', 'warn')
             + self.cs(' normal')) * 3

        slen = len('danger' + 'warn' + ' normal') * 3
        self.assertEqual(slen, len(s))

        s *= 10
        slen *= 10
        self.assertEqual(slen, len(s))

    def test_rerender(self):

        print

        # re-render strutil.ColoredString
        s = 'danger rerender to warn'

        c = self.cs(s, 'danger')
        print 'colorize with "danger":', c

        c = self.cs(c, 'warn')
        print 'colorize with "warn"  :', c

    def test_colored_prompt(self):

        s = '[colored prompt]# '

        prompt = self.cs(s, color='optimal', prompt=True)
        not_prompt = self.cs(s, color='optimal', prompt=False)

        self.assertEqual(str(prompt)[0], '\001')
        self.assertEqual(str(prompt)[-1], '\002')
        self.assertEqual(len(str(prompt)), len(str(not_prompt)) + 4)

        prompt += 'whatever'
        not_prompt += 'whatever'
        self.assertEqual(len(str(prompt)), len(str(not_prompt)) + 4)

        prompt *= 3
        not_prompt *= 3
        self.assertEqual(len(str(prompt)), len(str(not_prompt)) + 4*3)
