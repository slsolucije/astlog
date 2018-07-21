# -*- coding: utf-8 -*-
from __future__ import unicode_literals, print_function
import sys
import logging
import re
import time
import traceback

import urwid
from urwid import (Text, Edit, AttrWrap, BoxWidget, ListWalker, ListBox,
                   SimpleListWalker, Divider, Filler, Frame, Columns, Pile,
                   connect_signal, RadioButton)

from astlog.reader import LogParser, LogParserError, parse_when

_bytes3 = str if sys.version_info < (3,0,0) else bytes

log = logging.getLogger('astlog.app')

palette = [
    ('no', 'default', 'default'),
    ('selection', 'white', 'dark blue'),
    ('find1', 'white', 'dark blue'),
    ('find2', 'white', 'dark green'),
    ('current', 'default', 'black'),
    ('key', 'dark red', 'light gray'),
    ('key2', 'light red', 'default'),
    ('bar', 'black', 'light gray'),
    ('line-no', 'dark gray,underline', ''),
    ('mute', 'dark gray', 'default'),
    ('sip-call-id', 'dark gray', 'default'),
    ('sip-sip-id', 'dark gray', 'default'),
    ('sip-intro', 'white,bold', 'default'),
    ('sip-retransmit', 'light red,standout', 'default'),
    ('sip-request', 'dark blue', 'default'),
    ('sip-response', 'dark cyan', 'default'),
    ('sip-invite', 'light green,bold', 'default'),
    ('sip-bye', 'light red,standout', 'default'),
    ('sip-method', 'yellow,bold', 'default'),
    ('sip-status', 'light cyan,bold', 'default'),
    ('sip-invite2', 'light green', 'default'),
    ('sip-bye2', 'dark red', 'default'),
    ('sip-method2', 'yellow', 'default'),
    ('sip-status2', 'light cyan', 'default'),
    ('sip-timeout', 'light red,bold,standout', 'default'),
    ('sip-addr', 'dark gray', 'default'),
    ('verbose', 'dark gray', 'default'),
    ('acall-id', 'dark gray', 'default'),
    ('channel', 'dark magenta', 'default'),
    ('channel-name', 'dark magenta', 'default'),
    ('channel-phone', 'default', 'default'),
    ('app-name', 'light magenta,bold', 'default'),
    ('dial-status', 'light cyan', 'default'),
    ('dial-busy', 'light cyan', 'default'),
    ('dial-no-answer', 'dark red', 'default'),
    ('dial-answered', 'light green', 'default'),
    ('dial-status2', 'dark cyan', 'default'),
    ('asterisk-app', 'light magenta,bold,standout', 'default'),
    ('elapsed', 'white,bold', 'default'),
    ('section', 'light blue', 'default'),
    ('warning', 'yellow,standout', 'default'),
    ('error', 'light red,standout', 'default'),
]

HELP = [
    [('key2', 'F3'), ' = go to Search window. Once inside search window, you '
                     'must press enter to apply the search criteria. '
                     'You must press enter for each field separately'],
    [('key2', 'F4'), ' = go to Results window'],
    [('key2', 'F6'), ' = go to Log window'],
    [('key2', 'F10'), ' = exit program'],
    [('key2', 'tab'), ' = cycle between windows'],
    [('key2', 'enter'), ' = jump to corresponding log line. Works inside '
                        'Overview section.'],
    [('key2', 'backspace'), ' = jump back'],
    [('key2', 'space'), ' = expand/collapse details. Works inside Overview '
                        'section and inside Log section for single-line SIP '
                        'messages'],
    [('key2', 'c'), ' = toggle display of channel log messages'],
    [('key2', 'f'), ' = display only the lines matching the search criteria'],
    [('key2', 'g'), ' = go to top'],
    [('key2', 'G'), ' = go to bottom'],
    [('key2', 'i'), ' = isolate single call flow from all the others. '
                    'Go to call overview, select a call an press "i"'],
    [('key2', 'I'), ' = undo last isolation'],
    [('key2', 'l'), ' = toggle display of log messages'],
    [('key2', 'n'), ' = go to next search string'],
    [('key2', 'N'), ' = go to previous search string'],
    [('key2', 's'), ' = show SIP messages inside log. Cycle between: '
                    'full SIP message, single line, nothing'],
    [('key2', 'S'), ' = show SIP ladders for all dialogs'],
]


def message_sequence_chart(dialog):
    # ## Line    Timestamp   10.10.10.1:5060                    10.10.10.194:5062
    # 00 0001478 2..35.058  0.000 |--------INVITE q440 (102 INVITE)-------->|
    # 01 0001597 2..35.072  0.014 |<--------100 Trying (102 INVITE)---------|
    # 02 0001838 2..35.322  0.264 |<-------180 Ringing (102 INVITE)---------|
    # 03 0002771 2..49.076 14.018 |--------CANCEL q440 (102 CANCEL)-------->|
    # 04 0002863 2..49.087 14.029 |<----------200 OK (102 CANCEL)-----------|
    # 05 0002974 2..49.396 14.338 |<--487 Request Terminated (102 INVITE)---|
    # 06 0002986 2..49.397 14.339 |-----------ACK q440 (102 ACK)----------->|

    if not dialog or not dialog.sip_list:
        return []

    addresses = []
    for sip in dialog.sip_list:
        if sip.sender_addr not in addresses:
            addresses.append(sip.sender_addr)
        if sip.recipient_addr not in addresses:
            addresses.append(sip.recipient_addr)

    def addr_index(find_addr):
        for j, a in enumerate(addresses):
            if a == find_addr:
                return j

    max_label = max(len(str(sip)) for sip in dialog.sip_list)
    w = max_label + max_label % 2 + 6

    before_ladder = len(dialog.sip_list[0].when) + 25
    markup = []
    remaining = 0
    for i, addr in enumerate(addresses):
        half = len(addr) / 2
        if i == 0:
            markup.append(('mute', '##  Line     Timestamp'))
            markup.append(' ' * (before_ladder - half - 22))
        if remaining:
            markup.append(' ' * (w - half - remaining))
        markup.append(('', addr))
        remaining = len(addr) - half

    markups = [markup]

    ladder = ('|' + ' ' * (w - 1)) * (len(addresses) - 1)
    ladder += '|'
    for i, sip in enumerate(dialog.sip_list):
        markup = [('mute', [
            '%02d  ' % i,
            ('line-no', '%07d' % (sip.line_no + 1)),
            '  %s  %8.3f  ' % (sip.when, sip.elapsed_sec),
        ])]

        si = addr_index(sip.sender_addr)
        ri = addr_index(sip.recipient_addr)

        d = abs(si - ri)
        if si > ri:
            arrow = '<' + '-' * (w * d - 2)
            arrow_pos = ri * w + 1
        else:
            arrow = '-' * (w * d - 2) + '>'
            arrow_pos = si * w + 1
        arrow_len = len(arrow)

        row = ladder[:arrow_pos]
        row += arrow
        row += ladder[arrow_pos + arrow_len:]

        label = str(sip)
        label_pos = (arrow_len - len(label)) / 2 + arrow_pos

        row1 = row[:label_pos]
        row2 = row[label_pos + len(label):]

        if row1:
            markup.append(row1)
        markup.append((get_sip_style(sip) + '2', label))
        if row2:
            markup.append(row2)

        markups.append(markup)

    return markups


def sip_payload_instructions(sip):
    instructions = []
    line_no = sip.line_no
    style = 'sip-request' if sip.request else 'sip-response'
    for i, line in enumerate(sip.header):
        if i == 0:
            w1, dummy, w2 = line.partition(' ')
            if w1 == 'SIP/2.0':
                markup = [w1, ' ', ('sip-status', w2)]
            else:
                markup = [(get_sip_style(sip), w1), (style, ' ' + w2)]
            instructions.append((line_no, style, markup))
        else:
            instructions.append((line_no, style, line))
        line_no += 1
    if sip.body:
        instructions.append((line_no, '', ''))
        line_no += 1
        for line in sip.body:
            instructions.append((line_no, style, line))
            line_no += 1
    return instructions


def dial_chart(channel):
    timeline = []
    for extension, (line_no, when) in channel.extensions.items():
        timeline.append((line_no, when, 'e', extension))
    for app in channel.apps:
        timeline.append((app.line_no, app.when, 'd', app))
    timeline.sort()

    def ela(timestamp, wh):
        if timestamp:
            ts2 = parse_when(wh)
            if ts2:
                elapsed = ts2 - timestamp
                return elapsed.seconds + elapsed.microseconds / 1000000.0
        return 0

    markups = []
    if timeline:
        ts_len = len(timeline[0][1]) - 6
        markups.append([
            ('mute', '    Line     Timestamp'),
            ('mute', ' ' * ts_len),
            ('mute', 'Elapsed  Elapsed2  App          Data')
        ])
        if channel.clid_num:
            markups.append([
                ('mute', ' ' * (ts_len+41)),
                ('mute', 'CallerID     '),
                ('channel-phone', '%-13s' % channel.clid_num),
                ('', ' '),
                ('channel-phone', channel.clid_name or ' '),
            ])

    ts0 = parse_when(channel.when)
    for line_no, when, what, obj in timeline:
        if what == 'e':
            ela0 = ela(ts0, when)
            markups.append([
                ('', '    '),
                ('line-no', '%07d' % (line_no + 1)),
                ('mute', '  %s  %8.3f  ' % (when, ela0)),
                # ('mute', '  %s  ' % when),
                ('mute', '          Exten        '),
                ('channel-phone', obj),
            ])
        else:
            app = obj
            markups.append([
                ('', '    '),
                ('line-no', '%07d' % (line_no + 1)),
                ('mute', '  %s                      ' % when),
                ('app-name', '%-13s' % app.app_name),
                ('channel-phone', app.data),
                ('', ' -> '),
                (get_dial_status_style(app.status), app.status),
            ])
            ts = parse_when(app.when)
            for line_no2, when2, state, phone, dev_or_chan in app.log:
                ela0 = ela(ts0, when2)
                ela2 = ela(ts, when2)
                markups.append([
                    ('', '    '),
                    ('line-no', '%07d' % (line_no2 + 1)),
                    ('mute', '  %s  %8.3f  %8.3f    ' % (when2, ela0, ela2)),
                    (get_dial_status_style(state), '%-10s' % state),
                    ('channel-phone', ' %-13s' % (phone or '')),
                    ('mute', ' %s' % (dev_or_chan or '')),
                ])
    return markups


def get_sip_style(sip):
    if sip.request == b'INVITE':
        return 'sip-invite'
    elif sip.request == b'BYE':
        return 'sip-bye'
    elif sip.request:
        return 'sip-method'
    else:
        return 'sip-status'


def get_dial_status_style(status):
    if status == b'BUSY':
        return 'dial-busy'
    elif status == b'NO ANSWER':
        return 'dial-no-answer'
    elif status == b'ANSWERED':
        return 'dial-answered'
    else:
        return 'dial-status'


def ref_tag(dct):
    if dct:
        for ref_type in ['call_id', 'sip_ref', 'chan', 'acall_id']:
            if ref_type in dct:
                return ref_type, dct[ref_type]
    return None, None


class LineCollection(object):
    def __init__(self, search_map):
        self.search_map = search_map
        self.lines = []
        self.line_number_map = {}
        self.line_number_map_backup = {}
        self.line_numbers_with_needle = []
        self.match_count = 0
        self.filtered_lines = []
        self.all_lines = []

    def add(self, markup, line_no=None, needles=None, tag=None):
        if needles:
            search_map = self.search_map.copy()
            search_map.update(needles)
        else:
            search_map = self.search_map
        found = [False]
        for needle, style in search_map.items():
            finder = self._get_finder(needle)
            if finder:
                markup = self._search(markup, finder, style, line_no,
                                      found=found)
        if line_no is not None:
            self.line_number_map[line_no] = len(self.lines)
            self.line_number_map_backup[line_no] = len(self.lines)
            if isinstance(markup, list):
                row = [('line-no', '%07d' % (line_no + 1)), ' ', ('', markup)]
            else:
                row = [('line-no', '%07d' % (line_no + 1)), ' ', markup]
            self.lines.append((row, tag))
            self.all_lines.append((row, tag))
            if found[0]:
                self.filtered_lines.append((row, tag))
        else:
            self.lines.append((markup, tag))
            self.all_lines.append((markup, tag))
            if found[0]:
                self.filtered_lines.append((markup, tag))

    def insert(self, pos, markups):
        for i, markup in enumerate(markups):
            self.lines.insert(pos + i, (markup, None))
        for line_no, line_pos in self.line_number_map.items():
            if line_pos > pos:
                self.line_number_map[line_no] += len(markups)

    def remove(self, pos, count):
        self.lines[pos:pos + count] = []
        for line_no, line_pos in self.line_number_map.items():
            if line_pos > pos:
                self.line_number_map[line_no] -= count

    def set_filter(self, is_filter):
        if is_filter:
            self.lines[:] = self.filtered_lines or [
                ('0 lines matching search criteria', None)]
        else:
            self.line_number_map = self.line_number_map_backup.copy()
            self.lines[:] = self.all_lines

    # noinspection PyDefaultArgument
    def _search(self, markup, finder, style, line_no, level=0, max_depth=10,
                found=[False]):
        if level >= max_depth:
            return markup
        if isinstance(markup, basestring):
            text = markup
            start, end = finder(text)
            if end:
                sub_markup = []
                ll = self.line_numbers_with_needle
                if isinstance(style, dict):
                    jump = style.get('jump', False)
                    style = style['style']
                else:
                    jump = False
                while start >= 0:
                    if start > 0:
                        sub_markup.append(text[:start])
                    sub_markup.append((style, text[start:end]))
                    text = text[end:]
                    start, end = finder(text)
                    if jump and line_no is not None and \
                            not (ll and ll[-1] == line_no):
                        ll.append(line_no)
                        self.match_count += 1
                        found[0] = True
                if text:
                    sub_markup.append(text)
                return sub_markup
            return text
        elif isinstance(markup, tuple):
            return markup[0], self._search(markup[1], finder, style, line_no,
                                           level + 1, max_depth, found)
        elif isinstance(markup, list):
            return [self._search(m, finder, style, line_no,
                                 level + 1, max_depth, found)
                    for m in markup]
        else:
            return markup

    @staticmethod
    def _get_finder(needle):
        if not needle:
            return None
        if hasattr(needle, 'search'):
            def regex_finder(text):
                m = needle.search(text)
                if m:
                    return m.start(0), m.end(0)
                return None, None

            return regex_finder
        else:
            def string_finder(text):
                index = text.find(needle)
                if index >= 0:
                    return index, index + len(needle)
                return None, None

            return string_finder


class EnterEdit(Edit):
    signals = ['enter']

    def keypress(self, size, key):
        if key == 'enter':
            self._emit('enter', self.edit_text)
        return super(EnterEdit, self).keypress(size, key)


class Sidebar(BoxWidget):
    signals = ['select', 'search']

    def __init__(self, phones):
        self.phone_edit = EnterEdit('Phone:', '')
        self.chan_edit = EnterEdit('Chanl:', '')
        self.call_id_edit = EnterEdit('SipID:', '')
        self.text_edit = EnterEdit('Find:', '')
        connect_signal(self.phone_edit, 'enter', self.on_change, 'phone')
        connect_signal(self.chan_edit, 'enter', self.on_change, 'chan')
        connect_signal(self.call_id_edit, 'enter', self.on_change, 'call_id')
        connect_signal(self.text_edit, 'enter', self.on_change, 'text')
        self.phones_text = Text([('key', 'F4'), ' Phones'])
        self.head = Pile([
            AttrWrap(Text([('key', 'F3'), ' Search']), 'bar'),
            AttrWrap(self.phone_edit, 'no', 'selection'),
            AttrWrap(self.chan_edit, 'no', 'selection'),
            AttrWrap(self.call_id_edit, 'no', 'selection'),
            Divider('-'),
            AttrWrap(self.text_edit, 'no', 'selection'),
            AttrWrap(self.phones_text, 'bar'),
        ])
        self.items = SimpleListWalker([])
        self.set_results(phones)
        self.listbox = ListBox(self.items)
        self.frame = Frame(self.listbox, header=self.head)

    def set_results(self, results):
        self.phones_text.set_text([('key', 'F4'),
                                   ' Results (%s)' % len(results)])
        self.items[:] = []
        group = []
        for ref in results:
            item = RadioButton(group, ref, state=False)
            connect_signal(item, 'change', self.on_select_phone, ref)
            item = AttrWrap(item, 'no', 'selection')
            self.items.append(item)

    def render(self, size, focus=False):
        return self.frame.render(size, focus)

    def keypress(self, size, key):
        return self.frame.keypress(size, key)

    # noinspection PyUnusedLocal
    def on_select_phone(self, button, new_state, call_id):
        if new_state:
            self._emit('select', call_id)

    # noinspection PyUnusedLocal
    def on_change(self, widget, value, field_name):
        self._emit('search', field_name, value)


class LogText(Text):
    signals = ['jump', 'expand', 'isolate']
    my_tag = None

    def selectable(self):
        return True

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def keypress(self, size, key):
        if key == 'enter':
            line_no = self.get_text_by_style('line-no')
            try:
                self._emit('jump', int(line_no) - 1)
            except:
                pass
        elif key == 'backspace':
            self._emit('jump', -1)
        elif key == 'n':
            self._emit('jump', 'next')
        elif key == 'N':
            self._emit('jump', 'previous')
        elif key in ('g', 'home'):
            self._emit('jump', 'home')
        elif key in ('G', 'end'):
            self._emit('jump', 'end')
        elif key in (' ', 'i'):
            if self.my_tag:
                event = 'expand' if key == ' ' else 'isolate'
                log.debug('%s %s', event, self.my_tag)
                self._emit(event, self.my_tag)
        else:
            return key

    def find_style_slices(self, style):
        slices = []
        start = 0
        for sty, length in self.attrib:
            if sty == style:
                slices.append((start, start + length))
            start += length
        return slices

    def get_text_by_style(self, style):
        slices = self.find_style_slices(style)
        if slices:
            return self.text[slices[0][0]:slices[0][1]]


class LogLineWalker(ListWalker):
    """ListWalker-compatible class for lazily reading file contents."""

    def __init__(self, lines, jump=None, expand=None, isolate=None):
        self.lines = lines
        self.jump = jump
        self.expand = expand
        self.isolate = isolate
        self.focus = 0
        self.cursor_pos = 1

    def get_focus(self):
        return self._get_at_pos(self.focus)

    def set_focus(self, focus):
        self.focus = focus
        self._modified()

    def get_next(self, start_from):
        return self._get_at_pos(start_from + 1)

    def get_prev(self, start_from):
        return self._get_at_pos(start_from - 1)

    def _get_at_pos(self, pos):
        """Return a widget for the line phone passed."""
        if pos < 0:
            # line 0 is the start of the file, no more above
            return None, None
        elif pos < len(self.lines):
            # we have that line so return it
            obj, tag = self.lines[pos]
            if isinstance(obj, (basestring, tuple, list)):
                log_text = LogText(obj or '', wrap=urwid.CLIP)
                log_text.my_tag = tag
                connect_signal(log_text, 'jump', self.on_jump)
                connect_signal(log_text, 'expand', self.on_expand)
                connect_signal(log_text, 'isolate', self.on_isolate)
                return AttrWrap(log_text, '', 'current'), pos
            else:
                return obj, pos
        else:
            # no more lines
            return None, None

    # noinspection PyUnusedLocal
    def on_jump(self, widget, where):
        if self.jump:
            self.jump(where)

    # noinspection PyUnusedLocal
    def on_expand(self, widget, tag):
        if self.expand:
            self.expand(tag)

    # noinspection PyUnusedLocal
    def on_isolate(self, widget, tag):
        if self.isolate:
            self.isolate(tag)


class LogDisplay(BoxWidget):
    def __init__(self, parser, encoding):
        self.parser = parser
        self.encoding = encoding
        self.find = {'ref': '', 'text': ''}
        self.isolate_filter = (None, None)
        self.line_collection = None
        self.jump_stack = []
        self.expansions = {}
        self.show_sip_level = 2
        self.show_ladder = False
        self.show_only_filtered = False
        self.show_verbose = True
        self.show_channel = True
        self.line_no_before_isolate = 0
        self.showing_help = False

        self.walker = LogLineWalker([('Select call on the left side', None)],
                                    self.jump, self.expand, self.isolate)
        self.header = AttrWrap(Text([('key', 'F6'), ' Log']), 'bar')
        self.listbox = ListBox(self.walker)
        self.frame = Frame(self.listbox, header=self.header)

    def render(self, size, focus=False):
        return self.frame.render(size, focus)

    def keypress(self, size, key):
        return self.frame.keypress(size, key)

    def set_line_collection(self, line_collection):
        self.jump_stack = []
        self.line_collection = line_collection
        self.walker.lines = line_collection.lines
        self.walker.set_focus(0)
        lc, mc = len(line_collection.lines), line_collection.match_count
        self.header.set_text([
            ('key', 'F6'), ' Log lines: %d, matches: %s | ' % (lc, mc),
            ('key', 's'), '/', ('key', 'S'), ' toggle sip messages/ladders | ',
            ('key', 'n'), '/', ('key', 'N'), ' next/previous | ',
            ('key', 'f'), ' toggle filter | ',
            ('key', 'i'), '/', ('key', 'I'), ' isolate/un-isolate | ',
            ('key', 'enter'), '/', ('key', 'backspace'), ' jump/back | ',
            ('key', 'space'), ' expand/collapse',
        ])
        self.line_collection.set_filter(self.show_only_filtered)

    def help(self):
        if self.showing_help:
            self.refresh_log()
        else:
            lc = LineCollection({})
            for markup in HELP:
                lc.add(markup)
            self.set_line_collection(lc)
        self.showing_help = not self.showing_help

    @property
    def compiled_find(self):
        find_text = self.find['text']
        if find_text and len(find_text) > 2:
            try:
                find_text = re.compile(find_text)
            except Exception as e:
                log.debug('Invalid RE %r: %s', find_text, e)

        # noinspection PyDictCreation
        find_map = {self.find['ref']: 'find1'}
        # possibly override
        find_map[find_text] = {'style': 'find2', 'jump': True}
        return find_map

    def load_result(self, ref):
        self.isolate_filter = (None, None)
        self.find['ref'] = ref
        self.refresh_log()

    def toggle_filter(self):
        if not self.line_collection or self.listbox.get_focus()[0] is None:
            return
        self.jump_stack = []
        self.expansions = {}
        self.show_only_filtered = not self.show_only_filtered
        self.listbox.set_focus(0)
        self.line_collection.set_filter(self.show_only_filtered)
        self.listbox.set_focus(0)

    def toggle_sip(self):
        self.jump_stack = []
        self.expansions = {}
        self.show_sip_level = (self.show_sip_level + 1) % 3

    def jump(self, where):
        cur = self.listbox.get_focus()[1]
        if where == 'home':
            self.listbox.set_focus(0)
        elif where == 'end':
            self.listbox.set_focus(len(self.line_collection.lines) - 1)
        elif where == 'next':
            if self.show_only_filtered:
                return
            for line_no in self.line_collection.line_numbers_with_needle:
                pos = self.line_collection.line_number_map.get(line_no)  # + 1
                if pos > cur:
                    self.listbox.set_focus(pos)
                    break
        elif where == 'previous':
            if self.show_only_filtered:
                return
            collection = self.line_collection.line_numbers_with_needle
            for i in range(len(collection) - 1, -1, -1):
                line_no = collection[i]
                pos = self.line_collection.line_number_map.get(line_no)  # + 1
                if pos < cur:
                    self.listbox.set_focus(pos)
                    break
        elif where == -1:
            # Jump back from where we came
            if self.jump_stack:
                pos = self.jump_stack.pop()
                self.listbox.set_focus(pos)
        else:
            pos = self.line_collection.line_number_map.get(where)
            if pos and pos != cur:
                self.jump_stack.append(cur)
                self.listbox.set_focus(pos)

    def expand(self, tag):
        if self.show_ladder:
            return  # No effect when showing all ladders

        cur = self.listbox.get_focus()[1]

        ref_type, ref = ref_tag(tag)
        # Collapse
        if ref in self.expansions:
            old, markups = self.expansions[ref]
            self.line_collection.remove(old, len(markups))
            del self.expansions[ref]
            self.listbox.set_focus(cur)
            return

        # Expand
        markups = []
        if ref_type == 'call_id':
            dialog = self.parser.find_obj(ref_type, ref)
            markups = message_sequence_chart(dialog)
            markups = [[' ' * 9] + m for m in markups]  # indent
        elif ref_type == 'sip_ref':
            sip = self.parser.find_obj(ref_type, ref)
            instructions = sip_payload_instructions(sip)
            markups = [(s, m) for (_l, s, m) in instructions]
        elif ref_type == 'chan':
            channel = self.parser.find_obj(ref_type, ref)
            markups = dial_chart(channel)
            markups = [[' ' * 9] + m for m in markups]  # indent

        if markups:
            self.expansions[ref] = (cur + 1, markups)
            self.line_collection.insert(cur + 1, markups)
            self.listbox.set_focus(cur)

    def isolate(self, tag):
        ref_type, ref = ref_tag(tag)
        if ref_type and ref:
            self.line_no_before_isolate = self.listbox.get_focus()[1]
            self.isolate_filter = (ref_type, ref)
            self.refresh_log()
        else:
            self.isolate_filter = (None, None)
            self.refresh_log(focus_line=self.line_no_before_isolate)

    def refresh_log(self, focus_line=False):

        groups, objects = self.parser.get_linked_objects(
            self.find['ref'], self.isolate_filter)

        old_pos = self.listbox.get_focus()[1]

        warning_count = 0
        error_count = 0

        lc = LineCollection(self.compiled_find)
        if objects:

            # Prepare markup for "Overview" section

            lc.add([('section', 'Overview ')])  # must be a list
            for group in groups:
                tab, tab2 = '= ', '   '
                for line_no, what, obj in group.overview:
                    markup = obj
                    dialog = None
                    tag = None
                    if what == 'dialog':
                        sip = obj
                        dialog = sip.dialog
                        style = get_sip_style(sip)
                        if dialog:
                            dialog_status = dialog.dialog_status or ''
                            dialog_ack = dialog.dialog_ack or ''
                            dialog_bye = dialog.bye_addr or ''
                        else:
                            dialog_status = ''
                            dialog_ack = ''
                            dialog_bye = ''
                        markup = [
                            ('', ' '),
                            ('line-no', '%07d' % (line_no + 1)),
                            ('', ' '),
                            ('mute', '[%s] ' % sip.when),
                            (style, sip.request or sip.status or '?'),
                            ('', ' %s -> %s ' % (sip.from_num, sip.to_num)),
                            ('sip-status', '%s' % (dialog_status or ' ')),
                            ('', ' '),
                            ('sip-method', '%s' % (dialog_ack or ' ')),
                        ]
                        if dialog and dialog.timeout:
                            markup.extend([
                                ('', ' '),
                                ('sip-timeout', 'TIMEOUT')
                            ])
                        if dialog_bye:
                            markup.extend([
                                ('', ' '),
                                ('sip-bye2', 'BYE'),
                                ('mute', ' from '),
                                ('mute', dialog_bye),
                            ])
                        markup.append(('sip-call-id', '  %s' % sip.call_id))
                        tag = {'call_id': sip.call_id}
                    elif what == 'channel':
                        channel = obj
                        markup = [
                            ('line-no', '%07d' % (line_no + 1)),
                            ('', ' '),
                            ('mute', '[%s] ' % channel.when),
                            ('channel-name', channel.name)
                        ]
                        for phone in set(channel.extensions):
                            markup.append(('', ' '))
                            markup.append(('', phone))
                        if channel.clid_num:
                            markup.append(('channel-phone',
                                           ' (clid:%s)' % channel.clid_num))
                        for i, app in enumerate(channel.apps):
                            dial_style = get_dial_status_style(app.status)
                            markup.append(('mute', '; ' if i else ': '))
                            markup.append(('mute', '%s ' % app.app_name))
                            markup.append(('', app.data or ' '))
                            markup.append(('mute', ' '))
                            markup.append((dial_style, app.status))
                        tag = {'chan': channel.name}
                    elif what == 'astcall':
                        acall = obj
                        markup = [
                            ('line-no', '%07d' % (line_no + 1)),
                            ('', ' '),
                            ('mute', '[%s] ' % acall.when),
                            ('acall-id', acall.acall_id)
                        ]
                        tag = {'acall_id': acall.acall_id}
                    lc.add(['%s%s%s ' % (tab, what, tab2), markup or '?'],
                           tag=tag)

                    if dialog and dialog.timeout:
                        timeout_line_no, timeout_when = dialog.timeout
                        markup = [
                            ('', ' '),
                            ('line-no', '%07d' % (timeout_line_no + 1)),
                            ('mute', ' [%s] ' % timeout_when),
                            ('sip-timeout', 'TIMEOUT')
                        ]
                        lc.add(['%s%s%s ' % (tab, '      ', tab2), markup])

                    if dialog and self.show_ladder:
                        for markup in message_sequence_chart(dialog):
                            lc.add([' ' * 9] + markup)
                    tab, tab2 = '  - ', ' '
            lc.add('')

            # Prepare markup for "Log" section

            flat = objects.items()
            flat.sort()

            lc.add(('section', 'Log'))
            old_line_no = None
            for line_no, (style, obj) in flat:
                # Text is kept as bytes, convert to unicode only when displaying
                if isinstance(obj, _bytes3):
                    obj = obj.decode(self.encoding, errors='replace')
                if old_line_no is not None and old_line_no + 1 != line_no:
                    # If Missing single unimportant line, omit "..."
                    if not (style == 'sip' and old_line_no + 2 == line_no):
                        lc.add(('mute', '...'))
                old_line_no = line_no
                if style == 'sip':
                    if self.show_sip_level == 0:
                        continue
                    sip = obj
                    line_no = sip.line_no
                    tag = None
                    markup = [('sip-intro', '==== %s ' % sip.when),
                              ('elapsed', '(ela: %s) ' % sip.elapsed_sec)]
                    if self.show_sip_level == 1:
                        if sip.request:
                            markup.append((get_sip_style(sip), str(sip)))
                        else:
                            markup.append(('sip-status', str(sip)))
                        # Adding tag also allows message expansion
                        tag = {'sip_ref': sip.ref}
                    if sip.attempt_no:
                        markup.append(('sip-retransmit',
                                       'ATTEMPT #%s' % sip.attempt_no))
                    markup.append(('', ' %s ' % sip.direction))
                    # markup.append(('sip-direction', ' '))
                    if sip.direction == 'IN':
                        markup.append(('sip-addr', '%s' % sip.recipient_addr))
                        markup.append(('', ' <- '))
                        markup.append(('sip-addr', '%s' % sip.sender_addr))
                    else:
                        markup.append(('sip-addr', '%s' % sip.sender_addr))
                        markup.append(('', ' -> '))
                        markup.append(('sip-addr', '%s' % sip.recipient_addr))
                    lc.add(markup, tag=tag)
                    if self.show_sip_level == 1:
                        continue
                    instructions = sip_payload_instructions(sip)
                    # We use line_no again on purpose
                    # noinspection PyAssignmentToLoopOrWithParameter
                    for line_no, sty2, markup in instructions:
                        lc.add((sty2, markup), line_no)
                elif style == 'channel':
                    if self.show_channel:
                        lc.add((style, obj), line_no, {
                            'Dial': 'asterisk-app',
                            'Queue': 'asterisk-app',
                        })
                elif self.show_verbose:
                    if b'WARNING' in obj:
                        lc.add(('warning', obj), line_no)
                        warning_count += 1
                    if b'ERROR' in obj:
                        lc.add(('error', obj), line_no)
                        error_count += 1
                    else:
                        lc.add((style, obj), line_no)
                old_line_no = line_no

        if warning_count:
            lc.lines[0][0].append(('warning', ' Warnings: %d ' % warning_count))
        if error_count:
            lc.lines[0][0].append(('error', ' Errors: %d ' % error_count))

        self.set_line_collection(lc)

        if focus_line:
            focus_line = focus_line if isinstance(focus_line, int) else old_pos
            try:
                self.listbox.set_focus(focus_line, 'above')
            except IndexError:
                pass


class App(object):
    def __init__(self, parser, encoding):
        self.loop = None
        self.parser = parser
        self.panel_focus = 1
        self.show_ladder = False
        self.search = {
            'phone': '',
            'chan': '',
            'call_id': '',
        }

        self.sidebar = Sidebar(sorted(self.parser.get_phone_set()))
        connect_signal(self.sidebar, 'select', self.on_result_selected)
        connect_signal(self.sidebar, 'search', self.on_search)

        self.log_display = LogDisplay(parser, encoding)

        self.cols = Columns([
            ('fixed', 20, self.sidebar),
            ('fixed', 1, Filler(Divider(), 'top')),
            ('weight', 1, self.log_display),
        ])

        self.footer = Text('')
        self.set_footer_text()
        self.frame = Frame(self.cols, footer=AttrWrap(self.footer, 'bar'))

    def set_footer_text(self, markup=None):
        if markup:
            self.footer.set_text(markup)
        else:
            self.footer.set_text([
                'Press ',
                ('key', 'ESC'), '/', ('key', 'F10'), '/', ('key', 'q'),
                ' to exit, ',
                ('key', 'TAB'), '/', ('key', 'Fx'),
                ' to switch window, ',
                ('key', 'H'), ' for help',
            ])

    # noinspection PyUnusedLocal
    def on_result_selected(self, sidebar, ref):
        self.log_display.load_result(ref)

    def reload_file(self):
        start = time.time()
        old_pos = [None]

        def progress(module, line_no, data_pos, data_len):
            if data_pos == -1:
                self.sidebar.set_results(sorted(self.parser.get_phone_set()))
                self.set_footer_text()
            elif line_no % 10000 == 0:
                elapsed = time.time() - start
                rate = ''
                if old_pos[0] is None:
                    old_pos[0] = data_pos
                elif elapsed and module == 'log':
                    rate = (data_pos - old_pos[0]) / (time.time() - start)
                    rate = '%4.1fMB/s' % round(rate / 1024.0 / 1024.0, 2)
                self.set_footer_text('%s %s %5.1f%% %3.0fsec %s' % (
                    module,
                    line_no,
                    100.0 * data_pos / data_len,
                    elapsed,
                    rate))
                self.loop.draw_screen()

        try:
            self.parser.load_file(progress)
        except LogParserError as e:
            self.set_footer_text(unicode(e))

    # noinspection PyUnusedLocal
    def on_search(self, sidebar, field_name, value):
        if field_name in ('phone', 'chan', 'call_id'):
            self.search[field_name] = value
            phones = self.parser.search(
                self.search['phone'],
                self.search['chan'],
                self.search['call_id'],
            )
            self.sidebar.set_results(phones)
        elif field_name == 'text':
            self.log_display.find['text'] = value
            self.log_display.refresh_log(focus_line=True)

    def unhandled_keypress(self, key):
        # noinspection PyProtectedMember
        def focus_sidebar_header():
            self.sidebar.frame.focus_part = 'header'
            self.cols.set_focus_column(0)
            self.sidebar.head._invalidate()
            self.panel_focus = 0

        # noinspection PyProtectedMember
        def focus_sidebar_list():
            self.sidebar.frame.focus_part = 'body'
            self.sidebar.listbox._invalidate()
            self.cols.set_focus_column(0)
            self.panel_focus = 1

        def focus_log():
            self.cols.set_focus_column(2)
            self.panel_focus = 2

        if key in ('q', 'Q', 'esc', 'f10'):
            raise urwid.ExitMainLoop()
        elif key == 'tab':
            self.panel_focus = (self.panel_focus + 1) % 3
            if self.panel_focus == 0:
                focus_sidebar_header()
            elif self.panel_focus == 1:
                focus_sidebar_list()
            if self.panel_focus == 2:
                focus_log()
        elif key == 'f4':
            focus_sidebar_list()
        elif key == 'f3':
            focus_sidebar_header()
        elif key == 'f6':
            focus_log()
        elif key == 'c':
            self.log_display.show_channel = not self.log_display.show_channel
            self.log_display.refresh_log()
        elif key == 'l':
            self.log_display.show_verbose = not self.log_display.show_verbose
            self.log_display.refresh_log()
        elif key == 'S':
            self.log_display.show_ladder = not self.log_display.show_ladder
            self.log_display.refresh_log()
        elif key == 's':
            self.log_display.toggle_sip()
            self.log_display.refresh_log()
        elif key == 'f':
            self.log_display.toggle_filter()
        elif key == 'I':
            self.log_display.isolate(None)
        elif key in ('h', 'H'):
            self.log_display.help()

    def run(self):
        self.loop = urwid.MainLoop(self.frame, palette, handle_mouse=False,
                                   unhandled_input=self.unhandled_keypress)
        self.loop.set_alarm_in(0, lambda loop, data: self.reload_file())
        try:
            self.loop.run()
        except Exception:
            self.loop.stop()
            print(traceback.format_exc())


def main():

    import argparse
    parser = argparse.ArgumentParser(description='Analyze asterisk full log')
    parser.add_argument('log_file', help='path to full log')
    parser.add_argument('--cdr-file', dest='cdr_file',
                        help='optional CDR file (experimantal)')
    parser.add_argument('--from-when', dest='from_when',
                        help='Must be in form YYYY-MM-DD HH:MI:SS')
    parser.add_argument('--to-when', dest='to_when',
                        help='Must be in form YYYY-MM-DD HH:MI:SS')
    parser.add_argument('--tail-minutes', dest='tail_minutes', type=int)
    parser.add_argument('--log-output', dest='log_output')
    parser.add_argument('--use-memory-pct', dest='use_memory_pct', type=int)
    parser.add_argument('--encoding', dest='encoding', default='utf-8',
                        help='log_file encoding')

    args = parser.parse_args()

    if (args.from_when or args.to_when) and args.tail_minutes:
        raise Exception('--tail-minutes cannot be used with '
                                   '--from-when/--to-when')

    if args.log_output:
        logging.basicConfig(filename=args.log_output, level=logging.DEBUG,
                            format='%(asctime)s %(levelname)s %(message)s')
        log.debug('astlog started')

    try:
        log_parser = LogParser(args.log_file, args.cdr_file, args.from_when,
                               args.to_when, args.tail_minutes,
                               args.use_memory_pct)
        app = App(log_parser, args.encoding)
        app.run()
    except LogParserError as e:
        log.exception('parse error')
        print(str(e))
    except Exception as e:
        log.exception('bug')
        print(str(e))
    log.debug('astlog finished')


if __name__ == '__main__':

    main()

    # import pdb
    # import gc
    # from urwid import CanvasCache
    # CanvasCache.clear()
    # gc.collect()
    # pdb.set_trace()

# 012016160 -> 24c08af378821680537808390855bfe8@10.10.10.1:5060
