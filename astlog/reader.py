# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import os
import re
import logging
import csv
from datetime import datetime, timedelta


try:
    from cStringIO import StringIO
except ImportError:
    from io import StringIO

log = logging.getLogger('astlog.reader')

TIMESTAMP_FORMATS = [
    '%Y-%m-%d %H:%M:%S.%f',
    '%Y-%m-%d %H:%M:%S',
    '%b %d %H:%M:%S.%f',
    '%b %d %H:%M:%S',
]


def parse_when(when):
    for fmt in TIMESTAMP_FORMATS:
        try:
            return datetime.strptime(when, fmt)
        except ValueError:
            pass


class LogParserError(Exception):
    pass


class LogAstCall(object):
    __slots__ = ['acall_id', 'line_no', 'when', 'lines', 'sip_set',
                 'channel_set', 'call_id_set', 'current_dial', 'current_queue']

    def __init__(self, acall_id, line_no, when):
        self.acall_id = acall_id
        self.line_no = line_no
        self.when = when
        self.lines = []  # [(line_no, line), ...]
        self.sip_set = set()
        self.channel_set = set()
        self.call_id_set = set()
        self.current_dial = None
        self.current_queue = None


class SipDialog(object):
    __slots__ = ['call_id', 'sip_list', 'request', 'dialog_status',
                 'dialog_ack',
                 'is_establishing', 'was_established', 'had_bye', 'bye_addr',
                 'timeout']

    def __init__(self, call_id, sip):
        self.call_id = call_id
        self.sip_list = []
        self.request = sip.request
        self.dialog_status = None
        self.dialog_ack = None
        self.is_establishing = (sip.request == b'INVITE')
        self.was_established = False
        self.had_bye = False
        self.bye_addr = None
        self.timeout = None  # tuple (line_no, when)

    def add_sip(self, sip):
        if sip.request == b'INVITE':
            # We can have multiple INVITE messages -> reset
            self.is_establishing = True
        elif self.is_establishing:
            if sip.status:
                self.dialog_status = sip.status
            elif sip.request == b'ACK':
                self.dialog_ack = sip.request
                self.is_establishing = False
                if self.dialog_status:
                    if self.dialog_status.startswith(b'1'):
                        self.was_established = True
                    elif self.dialog_status.startswith(b'2'):
                        self.was_established = True
        elif self.was_established and not self.had_bye:
            if sip.request == b'BYE':
                self.bye_addr = sip.sender_addr
                self.had_bye = True
        elif self.request != b'INVITE' and sip.status:
            self.dialog_status = sip.status

        self.sip_list.append(sip)

    @property
    def start_sip(self):
        return self.sip_list[0] if self.sip_list else None

    @property
    def finish_sip(self):
        return self.sip_list[-1] if len(self.sip_list) > 1 else None


class SipMessage(object):
    __slots__ = ['line_no', 'direction', 'peer_addr', 'attempt_no', 'is_nat',
                 'when', 'header', 'body', 'request', 'request_addr',
                 'status', 'intro_line',
                 'from_name', 'from_num', 'from_addr',
                 'to_name', 'to_num', 'to_addr',
                 'via_addr', 'sender_addr', 'recipient_addr',
                 'cseq', 'call_id', 'acall', 'dialog', 'request_sip',
                 '_where', '_timestamp']

    def __init__(self, line_no, direction, peer_addr, is_nat, when,
            acall, intro_line):
        self.line_no = line_no
        self.direction = direction
        self.peer_addr = peer_addr
        self.is_nat = is_nat
        self.when = when
        self.acall = acall
        self.intro_line = intro_line
        self.attempt_no = 0
        self.header = []
        self.body = []
        self._where = 0
        self.dialog = None
        self.request = None
        self.request_addr = None
        self.status = None
        self.from_name = None
        self.from_num = None
        self.from_addr = None
        self.to_name = None
        self.to_num = None
        self.to_addr = None
        self.call_id = None
        self.cseq = None
        self.via_addr = None
        self.request_sip = None
        self.sender_addr = None
        self.recipient_addr = None
        self._timestamp = None

    def __str__(self):
        att = b' #%s' % self.attempt_no if self.attempt_no else b''
        if self.request:
            return b'%s %s (%s)%s' % (self.request, self.to_num, self.cseq, att)
        else:
            return b'%s (%s)%s' % (self.status, self.cseq, att)

    def __repr__(self):
        if self.request:
            return b'%s %s (line: %s)' % (self.request, self.request_addr,
                                          self.line_no)
        else:
            return b'%s (line: %s)' % (self.status, self.line_no)

    @property
    def ref(self):
        return b'%s/%s' % (self.call_id, self.line_no + 1)

    @property
    def timestamp(self):
        if self._timestamp is None:
            self._timestamp = parse_when(self.when)
        return self._timestamp

    @property
    def elapsed(self):
        if self.dialog \
                and self.dialog.start_sip \
                and self.dialog.start_sip.timestamp \
                and self.timestamp:
            return self.timestamp - self.dialog.start_sip.timestamp

    @property
    def elapsed_sec(self):
        elapsed = self.elapsed
        if elapsed:
            return elapsed.seconds + elapsed.microseconds / 1000000.0
        return 0.0

    def add_line(self, line):
        if line.startswith(b'<--') or line.startswith(b'---'):
            return False
        if self._where == 0:
            if line:
                self.add_header(line)
            else:
                self._where = 1
            return True
        elif self._where == 1:
            if line:
                self.body.append(line)
                self._where = 2
                return True
            else:
                return False
        elif self._where == 2:
            if line:
                self.body.append(line)
            else:
                self._where = 3
            return True
        else:
            if line:
                self.body.append(b'')
                self.body.append(line)
                self._where = 2
                return True
            else:
                return False

    def add_header(self, line):
        if not self.header:
            if line.startswith(b'SIP/2.0'):
                self.status = line[8:]
            else:
                pos = line.index(b' ')
                self.request = line[:pos]
                self.request_addr, pos = delimited(line, b'sip:', b' ', pos)
                pos = self.request_addr.find(b'@')
                if pos > 0:
                    self.request_addr = self.request_addr[pos + 1:]
                pos = self.request_addr.find(b';')
                if pos > 0:
                    self.request_addr = self.request_addr[:pos]
        else:
            if line.startswith(b'From:'):
                self.from_name, self.from_num, self.from_addr = \
                    parse_from_to(line, 6)
            elif line.startswith(b'To:'):
                self.to_name, self.to_num, self.to_addr = \
                    parse_from_to(line, 4)
            elif line.startswith(b'Call-ID:'):
                self.call_id = line[9:]
            elif line.startswith(b'Via:'):
                self.via_addr, pos = delimited(line, b' ', b';', 14)
            elif line.startswith(b'CSeq:'):
                self.cseq = line[6:]
        self.header.append(line)

    def finalize_sip(self):
        if not self.request and self.dialog:
            # Find request for this response
            for i in range(len(self.dialog.sip_list) - 1, -1, -1):
                s = self.dialog.sip_list[i]
                if s.request and s.cseq == self.cseq:
                    self.request_sip = s
                    break

        # req/res   dir  sender                  recipient
        # --------  ---  ----------------------  --------------
        # REQUEST   IN   via                     to, bye header
        # REQUEST   OUT  via                     req url
        # RESPONSE  IN   prev request recipient  via
        # RESPONSE  OUT  to                      via
        if self.request:
            if self.direction == 'IN':
                self.sender_addr = self.via_addr
                self.recipient_addr = self.to_addr or self.request_addr
            else:
                self.sender_addr = self.via_addr
                self.recipient_addr = self.request_addr
        else:
            if self.direction == 'IN':
                if self.request_sip:
                    self.sender_addr = self.request_sip.recipient_addr
                self.recipient_addr = self.via_addr
            else:
                self.sender_addr = self.to_addr
                self.recipient_addr = self.via_addr

    @property
    def dialog_finish_sip(self):
        return self.dialog.finish_sip if self.dialog else None


class LogChannel(object):
    __slots__ = ['name', 'line_no', 'when', 'apps', 'extensions', 'lines',
                 'acall_set', 'sip_set', 'current_dial', 'current_queue',
                 'clid_name', 'clid_num']

    def __init__(self, name, line_no, when):
        self.line_no = line_no
        self.name = name
        self.when = when
        self.apps = []
        self.extensions = {}
        self.lines = []
        self.acall_set = set()
        self.sip_set = set()
        self.current_dial = None
        self.current_queue = None
        self.clid_name = None
        self.clid_num = None

    def add_acall(self, acall):
        self.acall_set.add(acall)

    def add_extension(self, extension, line_no, when):
        if extension not in self.extensions:
            self.extensions[extension] = line_no, when

    def start_dial(self, line_no, when, extension, app_data):
        phones = []
        idx = app_data.find(b',')
        if idx > 0:
            app_data = app_data[:idx]
        for device_name in app_data.split(b'&'):
            phone = device_phone(device_name)
            phones.append(phone)
        self.current_dial = LogDial(self, line_no, when, extension, phones)
        self.apps.append(self.current_dial)
        return self.current_dial

    def start_queue(self, line_no, when, extension, app_data):
        queue_name = app_data
        self.current_queue = LogQueue(self, line_no, when, extension,
                                      queue_name)
        self.apps.append(self.current_queue)
        return self.current_queue

    @property
    def phones(self):
        return set(self.extensions) | self.dialed_phones

    @property
    def dials(self):
        return [app for app in self.apps if isinstance(app, LogDial)]

    @property
    def queues(self):
        return [app for app in self.apps if isinstance(app, LogQueue)]

    @property
    def dialed_phones(self):
        return set(p for d in self.dials for p in d.phones)


class LogDial(object):
    app_name = 'Dial'
    __slots__ = ['channel', 'line_no', 'when', 'extension', 'phones', 'log',
                 'status', '_was_busy']

    def __init__(self, channel, line_no, when, extension, phones):
        self.channel = channel
        self.line_no = line_no
        self.when = when
        self.extension = extension
        self.phones = phones
        self.log = []
        self.status = b'ACTIVE'
        self._was_busy = False

    @property
    def data(self):
        return b', '.join(self.phones)

    def called(self, line_no, when, device_name):
        phone = device_phone(device_name)
        self.log.append((line_no, when, b'CALL', phone, device_name))

    def ringing(self, line_no, when, chan):
        phone = channel_phone(chan)
        self.log.append((line_no, when, b'RINGING', phone, chan))
        if self.status == b'ACTIVE':
            self.status = b'RINGING'

    def busy(self, line_no, when, chan):
        phone = channel_phone(chan)
        self.log.append((line_no, when, b'BUSY', phone, chan))
        self._was_busy = True

    def progress(self, line_no, when, chan1, chan2):
        if chan1 == self.channel.name:
            phone = channel_phone(chan2)
            self.log.append((line_no, when, b'PROGRESS', phone, chan2))
        else:
            log.debug(
                'LogDial progress wrong channel %s instead of %s: line %s',
                chan1, self.channel.name, line_no + 1)

    # noinspection PyUnusedLocal
    def pickup(self, line_no, when, ringing_chan, picked_by_chan):
        phone = channel_phone(ringing_chan)
        self.log.append((line_no, when, b'PICKUP', phone, ringing_chan))
        self.status = b'PICKUP'

    def answered(self, line_no, when, chan1, chan2):
        if chan1 == self.channel.name:
            phone = channel_phone(chan2)
            self.log.append((line_no, when, b'ANSWERED', phone, chan2))
            self.status = b'ANSWERED'
        else:
            log.debug(
                'LogDial answered wrong channel %s instead of %s: line %s',
                chan1, self.channel.name, line_no + 1)

    def manager_hangup(self, line_no, when, chan):
        if chan == self.channel.name:
            self.log.append((line_no, when, b'HANGUP', b'manager', chan))
            self.finish()
        else:
            log.debug('LogDial hangup wrong channel %s instead of %s: line %s',
                      chan, self.channel.name, line_no + 1)

    def extension_exited(self, line_no, when):
        self.log.append((line_no, when, b'EXIT', None, None))
        self.finish()

    def nobody_picked_up(self, line_no, when):
        self.log.append((line_no, when, b'NO ANSWER', None, None))
        self.channel.current_dial = None
        self.status = b'NO ANSWER'

    def finish(self):
        self.channel.current_dial = None
        if self.status == b'RINGING':
            self.status = b'NO ANSWER'
        elif self._was_busy:
            self.status = b'BUSY'
        elif self.status not in (b'ANSWERED', b'NO ANSWER', b'PICKUP'):
            self.status = b'EXIT'


class LogQueue(object):
    app_name = 'Queue'

    def __init__(self, channel, line_no, when, extension, queue_name):
        self.channel = channel
        self.line_no = line_no
        self.when = when
        self.extension = extension
        self.name = queue_name
        self.log = []
        self.status = b'ACTIVE'

    @property
    def data(self):
        return self.name

    def ringing(self, line_no, when, chan):
        phone = channel_phone(chan)
        self.log.append((line_no, when, b'RINGING', phone, chan))
        if self.status == b'ACTIVE':
            self.status = b'RINGING'

    def position(self, line_no, when, chan, position):
        if chan == self.channel.name:
            self.log.append((line_no, when, b'POSITION', position, chan))
        else:
            log.warn(
                'LogQueue position wrong channel %s instead of %s: line %s',
                chan, self.channel.name, line_no + 1)

    # noinspection PyUnusedLocal
    def pickup(self, line_no, when, ringing_chan, picked_by_chan):
        phone = channel_phone(ringing_chan)
        self.log.append((line_no, when, b'PICKUP', phone, ringing_chan))
        self.status = b'PICKUP'

    def answered(self, line_no, when, chan1, chan2):
        if chan1 == self.channel.name:
            phone = channel_phone(chan2)
            self.log.append((line_no, when, b'ANSWERED', phone, chan2))
            self.status = b'ANSWERED'
        else:
            log.warn(
                'LogQueue answered wrong channel %s instead of %s: line %s',
                chan1, self.channel.name, line_no + 1)


class LogParser(object):
    def __init__(self, filename, cdr_filename=None,
            from_when=None, to_when=None, tail_minutes=None,
            use_memory_pct=None):
        self.filename = filename
        self.cdr_filename = cdr_filename
        self.from_when = from_when
        self.to_when = to_when
        self.tail_minutes = tail_minutes
        self.use_memory_pct = min(max(use_memory_pct or 5, 5), 75)
        self.total_lines = 0
        self.acalls = {}
        self.call_lines = {}
        self.sip_messages = []
        self.call_timeouts = {}
        self.dialogs = {}
        self.channels = {}
        self.pickup_chans = {}  # who -> whom
        self.queues = {}
        # Links
        self.call_acall_map = {}
        self.call_sip_map = {}
        self.phone_sip_map = {}
        self.phone_channel_map = {}

        if not os.path.isfile(filename):
            raise LogParserError('No such file: %s' % filename)
        if cdr_filename and not os.path.isfile(cdr_filename):
            raise LogParserError('No such file: %s' % cdr_filename)

    def read_data(self):
        with open(self.filename, 'rb') as f:

            if self.tail_minutes:
                f.seek(-32000, os.SEEK_END)
                when, dummy = read_when(f.read(32000))
                if when:
                    late_ts = parse_when(when)
                    start_ts = late_ts - timedelta(minutes=self.tail_minutes)
                    self.from_when = str(start_ts)
                    self.to_when = None
                    log.info('set from_when=%s based on tail minutes %d',
                             self.from_when, self.tail_minutes)

            start_pos = 0
            if self.from_when:
                start_pos = find_file_position(f, self.from_when, 'after')
                if start_pos is None:
                    raise LogParserError('No data after %s' % self.from_when)
                log.info('reading from %s at offset %s',
                         self.from_when, start_pos)

            num_bytes = -1
            if self.to_when:
                finish_pos = find_file_position(f, self.to_when, 'before')
                if finish_pos is None:
                    raise LogParserError('No data before %s' % self.to_when)
                # get the line indicated by position, plus little more
                num_bytes = finish_pos - start_pos + 16000
                if num_bytes < 0:
                    raise LogParserError(
                        'Negative number of bytes is specified')
                log.info('reading %s bytes before %s', num_bytes, self.to_when)

            if num_bytes == -1:
                f.seek(0, os.SEEK_END)
                size = f.tell()
            else:
                size = num_bytes
            mem_size = get_memory_size()
            # Double the size because we store all data twice (raw + parsed)
            if mem_size and size * 2 > self.use_memory_pct * mem_size / 100:
                raise LogParserError('Refusing to analyse: to much data, '
                                     'more than %d%% of system memory.' %
                                     self.use_memory_pct)

            f.seek(start_pos, os.SEEK_SET)
            return f.read(num_bytes)

    def load_file(self, progress=None):
        data = self.read_data()
        first_when = None
        when = None
        data_len = len(data)
        data_pos = 0
        lines = []
        sip = None
        line_no = -1
        acall = None
        while data_pos < data_len:  # and line_no < 200000:
            line_no += 1
            line, data_pos = get_line(data, data_pos, data_len)
            lines.append((when, line))

            if progress:
                progress('log', line_no, data_pos, data_len)

            if sip:
                if not sip.add_line(line):
                    self.finish_sip(sip)
                    sip = None
                continue

            # Timestamp
            if line.startswith(b'['):
                pos = line.index(b']', 5)
                when = line[1:pos]
                if not first_when:
                    first_when = when

                # VERBOSE[nnn][C-1234567]
                acall, pos = self.link_acall(line_no, line, pos, when)

                # chan_sip.c:
                idx = line.find(b'chan_sip.c:', pos)
                if idx >= 0:
                    pos = idx + 11
                    sip = self.parse_chan_sip_c(line_no, line, pos, when, acall)
                    continue

                # pbx.c:
                idx = line.find(b'pbx.c:', pos)
                if idx >= 0:
                    pos = idx + 6
                    self.parse_pbx_c(line_no, line, pos, when, acall)
                    continue

                # app_dial.c:
                idx = line.find(b'app_dial.c:', pos)
                if idx >= 0:
                    pos = idx + 11
                    self.parse_app_dial_c(line_no, line, pos, when, acall)
                    continue

                # features.c:
                idx = line.find(b'features.c:', pos)
                if idx >= 0:
                    pos = idx + 11
                    self.parse_features_c(line_no, line, pos, when, acall)
                    continue

                # app_queue.c:
                idx = line.find(b'app_queue.c:', pos)
                if idx >= 0:
                    pos = idx + 12
                    self.parse_app_queue_c(line_no, line, pos, when, acall)
                    continue

                # app_queue.c:
                idx = line.find(b'manager.c:', pos)
                if idx >= 0:
                    pos = idx + 10
                    self.parse_manager_c(line_no, line, pos, when, acall)
                    continue

            # <--- SIP read from UDP:10.56.0.13:5060 --->
            elif line.startswith(b'<--- SIP read from'):
                peer_addr, idx = delimited(line, b":", b" ", 18)
                sip = self.sip(line_no + 1, 'IN', peer_addr, None, when, acall)

            # <--- Transmitting (no NAT) to 10.10.10.184:5062 --->
            elif line.startswith(b'<--- Transmitting'):
                peer_addr, idx = delimited(line, b" to ", b" ", 17)
                sip = self.sip(line_no + 1, 'OUT', peer_addr, b'(NAT)' in line,
                               when, acall)

            # <--- Reliably Transmitting (NAT) to 10.56.0.13:5060 --->
            elif line.startswith(b'<--- Reliably Transmitting'):
                peer_addr, idx = delimited(line, b" to ", b" ", 26)
                sip = self.sip(line_no + 1, 'OUT', peer_addr, b'(NAT)' in line,
                               when, acall)

        if sip:
            self.finish_sip(sip)
        self.total_lines = line_no + 1

        self.load_cdr(first_when, when, progress)

        if progress:
            progress('log', line_no, -1, data_len)

    def load_cdr(self, from_when, to_when, progress):
        if not self.cdr_filename:
            return
        phone_chan_map = {}
        caller_id_chan_map = {}
        with open(self.cdr_filename, 'rb') as f:
            start_pos = find_file_position(f, from_when, 'after', True)
            finish_pos = find_file_position(f, to_when, 'before', True)
            log.debug('CDR from_when=%s, start_pos=%s', from_when, start_pos)
            log.debug('CDR to_when=%s, finish_pos=%s', to_when, finish_pos)
            if not start_pos or not finish_pos:
                return

            data_len = finish_pos - start_pos + 2000000
            f.seek(max(0, start_pos - 1000000), os.SEEK_SET)
            reader = csv.reader(f)
            for row in reader:
                if progress:
                    progress('cdr', reader.line_num, f.tell(), data_len)
                if len(row) < 16:
                    continue
                src = row[1]
                dst = row[2]
                clid = row[4]
                idx = clid.find(b'<')
                if idx >= 0:
                    clid_name = clid[:idx].strip()
                    if clid_name.startswith(b'"'):
                        clid_name = clid_name[1:-1]
                    clid_num, dummy = delimited(clid, b'<', b'>', idx)
                else:
                    clid_name = None
                    clid_num = clid
                chan = row[5]
                dst_chan = row[6]
                c1map = phone_chan_map.setdefault(chan, set())
                c2map = phone_chan_map.setdefault(dst_chan, set())
                caller_id_chan_map[chan] = (clid_name, clid_num)
                c1map.add(src)
                c1map.add(dst)
                c1map.add(clid_num)
                c2map.add(src)
                c2map.add(dst)
                c2map.add(clid_num)
                if clid_name:
                    c1map.add(clid_name)
                    c2map.add(clid_name)
                if f.tell() > finish_pos + 1000000:
                    break
                log.debug('%s %s', c1map, f.tell())
        for chan, phones in phone_chan_map.items():
            channel = self.channels.get(chan)
            if channel:
                for phone in phones:
                    self.phone_channel_map.setdefault(phone, set()).add(channel)
        for chan, caller_id in caller_id_chan_map.items():
            channel = self.channels.get(chan)
            if channel:
                channel.clid_name, channel.clid_num = caller_id

    def sip(self, line_no, direction, peer_addr, is_nat, when, acall,
            intro_line=None):
        sip = SipMessage(line_no, direction, peer_addr, is_nat, when, acall,
                         intro_line)
        self.sip_messages.append(sip)
        return sip

    def finish_sip(self, sip):
        if sip.call_id:
            self.call_sip_map.setdefault(sip.call_id, []).append(sip)
            dialog = self.dialogs.get(sip.call_id)
            if not dialog:
                dialog = SipDialog(sip.call_id, sip)
                self.dialogs[sip.call_id] = dialog
            sip.dialog = dialog
            sip.finalize_sip()
            dialog.add_sip(sip)
            if sip.intro_line:
                # We didn't have call_id at the time of intro line
                self.link_call(sip.line_no - 1, sip.intro_line, sip.call_id,
                               sip.acall)
            dialog.timeout = self.call_timeouts.get(sip.call_id)
        if sip.acall:
            sip.acall.sip_set.add(sip)
        if sip.from_name:
            self.phone_sip_map.setdefault(sip.from_name, []).append(sip)
        if sip.from_num:
            self.phone_sip_map.setdefault(sip.from_num, []).append(sip)
        if sip.to_name:
            self.phone_sip_map.setdefault(sip.to_name, []).append(sip)
        if sip.to_num:
            self.phone_sip_map.setdefault(sip.to_num, []).append(sip)

    def parse_chan_sip_c(self, line_no, line, pos, when, acall):

        # Reliably Transmitting (no NAT) to 10.10.10.114:5062:
        idx = line.find(b'Reliably Transmitting', pos)
        if idx > 0:
            idx = line.find(b' to ', idx + 21)
            if idx > 0:
                peer_addr = line[idx + 4:-1]
                return self.sip(line_no + 1, 'OUT', peer_addr, b'(NAT)' in line,
                                when, acall, line)
            return

        # Transmitting (no NAT) to 10.10.10.191:5062:
        idx = line.find(b'Transmitting', pos)
        if idx > 0:
            idx = line.find(b' to ', idx + 12)
            if idx > 0:
                peer_addr = line[idx + 4:-1]
                return self.sip(line_no + 1, 'OUT', peer_addr, b'(NAT)' in line,
                                when, acall, line)
            return

        # Retransmitting #1 (no NAT) to 10.10.10.162:5062:
        idx = line.find(b'Retransmitting', pos)
        if idx > 0:
            attempt_num, idx = delimited(line, b"#", b" ", idx + 14)
            if attempt_num:
                idx = line.find(b' to ', idx)
                if idx > 0:
                    peer_addr = line[idx + 4:-1]
                    sip = self.sip(line_no + 1, 'OUT', peer_addr,
                                   b'(NAT)' in line, when, acall, line)
                    sip.attempt_no = attempt_num
                    return sip
            return

        # Really destroying SIP dialog
        # '151375913f3dc86668b0bba873bc646c@10.10.10.1:5060'
        # Method: OPTIONS
        idx = line.find(b'Really destroying SIP dialog', pos)
        if idx > 0:
            call_id, idx = delimited(line, b"'", b"'", idx + 28)
            if call_id:
                self.link_call(line_no, line, call_id, acall)
                # self.destroy_dialog(call_id, line, when)
            return

        # Scheduling destruction of SIP dialog '872487463@10.10.10.190'
        # in 6400 ms (Method: BYE)
        idx = line.find(b'Scheduling destruction of SIP dialog', pos)
        if idx > 0:
            call_id, idx = delimited(line, b"'", b"'", idx + 36)
            if call_id:
                self.link_call(line_no, line, call_id, acall)
                # self.schedule_dialog_destruction(call_id, line, when)
            return

        # Hanging up call 5321437f00000113ce0074abf20009b460@127.0.0.1
        # - no reply to our critical packet
        # (see https://wiki.asterisk.org/wiki/display/AST/SIP+Retransmissions).
        idx = line.find(b'Hanging up call', pos)
        if idx > 0:
            call_id, idx = delimited(line, b" ", b" ", idx + 15)
            if call_id:
                self.link_call(line_no, line, call_id, acall)
                # self.hanging_up(call_id, line, when)
            return

        # Retransmission timeout reached on transmission
        # 6ed9bfa20c3dad362194d8d359162070@10.55.0.226:5060 for seqno 103
        # (Critical Request)
        # -- See https://wiki.asterisk.org/wiki/display/AST/SIP+Retransmissions
        idx = line.find(b'Retransmission timeout reached on transmission', pos)
        if idx > 0:
            call_id, idx = delimited(line, b" ", b" ", idx + 46)
            if call_id:
                self.link_call(line_no, line, call_id, acall)
                self.retransmission_timeout(call_id, line_no, when)
            return

    def parse_pbx_c(self, line_no, line, pos, when, acall):
        # -- Auto fallthrough, chan 'SIP/322-0015bc14'
        # status is 'CHANUNAVAIL'
        idx = line.find(b'-- Auto fallthrough, chan', pos)
        if idx > 0:
            chan, idx = delimited(line, b"'", b"'", idx + 25)
            if chan:
                self.link_chan(line_no, line, chan, acall, when)
            return

        # -- Executing
        idx = line.find(b'-- Executing', pos)
        if idx > 0:
            # Executing [016445520@ctx1:2]
            # .. Dial("SIP/tk-0015b", "SIP/441&SIP/tk/123,14")
            extension, idx = delimited(line, b'[', b'@', idx + 12)
            app, idx = delimited(line, b'] ', b'(', idx)
            chan, idx = delimited(line, b'("', b'"', idx)
            channel = self.link_chan(line_no, line, chan, acall, when)
            self.phone_channel_map.setdefault(extension, set()).add(channel)
            channel.add_extension(extension, line_no, when)
            if app == b'Dial':
                app_data, idx = delimited(line, b'"', b'"', idx + 2)
                # Start Dial app
                dial = channel.start_dial(line_no, when, extension, app_data)
                for phone in dial.phones:
                    self.phone_channel_map.setdefault(phone, set()).add(channel)
                if acall:
                    acall.current_dial = dial
            elif app == b'Queue':
                app_data, idx = delimited(line, b'"', b'"', idx + 2)
                # Start Queue app
                queue = channel.start_queue(line_no, when, extension, app_data)
                if acall:
                    acall.current_queue = queue
                self.queues.setdefault(queue.name, []).append(queue)
            return

        # == Spawn extension (sub-gsm, tk1, 7)
        # exited non-zero on 'SIP/208-0015bcb7'
        idx = line.find(b'== Spawn extension', pos)
        if idx > 0:
            dial = acall and acall.current_dial
            if not dial:
                return
            idx = line.find(b'exited', idx)
            if idx > 0:
                chan, idx = delimited(line, b"'", b"'", idx)
                if chan:
                    dial.extension_exited(line_no, when)
                acall.current_dial = None
            return

    def parse_app_dial_c(self, line_no, line, pos, when, acall):
        dial = acall and acall.current_dial
        if not dial:
            return

        # -- Called SIP/440
        idx = line.find(b'-- Called', pos)
        if idx > 0:
            device = line[idx + 10:]
            dial.called(line_no, when, device)
            return

        # -- SIP/441-0015bc3d is ringing
        if line.endswith(b'is ringing'):
            chan, idx = delimited(line, b'-- ', b' ', pos)
            dial.ringing(line_no, when, chan)
            return

        # -- SIP/440-0015bc43 is busy
        if line.endswith(b'is busy'):
            chan, idx = delimited(line, b'-- ', b' ', pos)
            dial.busy(line_no, when, chan)
            return

        # -- SIP/gsm3-001501b is making progress passing it to SIP/202-001501a
        idx = line.find(b'is making progress passing it to', pos)
        if idx > 0:
            chan2, idx = delimited(line, b'-- ', b' ', pos)
            idx = line.find(b'it to ', idx)
            chan1 = line[idx + 6:]
            dial.progress(line_no, when, chan1, chan2)
            return

        # -- SIP/gsm2-0015bcb8 answered SIP/208-0015bcb7
        idx = line.find(b'answered', pos)
        if idx > 0:
            chan1 = line[idx + 9:]
            ans_by_chan, idx = delimited(line, b'-- ', b' ', pos)
            # Handle pickup (complicated!)
            if ans_by_chan in self.pickup_chans:
                pick_line_no, pick_line, pick_when, pick_chan = \
                    self.pickup_chans[ans_by_chan]
                dial.pickup(pick_line_no, pick_when, pick_chan, ans_by_chan)
                dial.channel.lines.append((pick_line_no, pick_line))
                # We don't have linked SIP because there is no hard link.
                # Attempt to link by finding interesting SIP messages
                # between pickup line number and answer line number
                phone = channel_phone(ans_by_chan)
                sip = self.find_ok_sip_from(phone, pick_line_no, line_no)
                if sip:
                    dial.channel.sip_set.add(sip)
            dial.answered(line_no, when, chan1, ans_by_chan)
            return

        # -- Nobody picked up
        idx = line.find(b'-- Nobody picked up', pos)
        if idx > 0:
            dial.nobody_picked_up(line_no, when)
            acall.current_dial = None
            return

        # == Everyone is busy
        idx = line.find(b'== Everyone is busy', pos)
        if idx > 0:
            dial.finish()
            return

    # noinspection PyUnusedLocal
    def parse_features_c(self, line_no, line, pos, when, acall):
        # pickup SIP/440-0015bd5a attempt by SIP/320-0015bd5f
        idx = line.find(b'pickup', pos)
        if idx > 0:
            target_chan, idx = delimited(line, b' ', b' ', idx + 6)
            if idx > 0:
                idx = line.find(b'attempt by ', idx)
                if idx > 0:
                    chan = line[idx + 11:]
                    self.pickup_chans[chan] = (line_no, line, when, target_chan)
            return

    def parse_app_queue_c(self, line_no, line, pos, when, acall):
        queue = acall and acall.current_queue
        if not queue:
            return

        # -- Nobody picked up
        # Ignore because it is fired multiple times, once for each "ringing"

        # -- SIP/440-0015bbf6 is ringing
        if line.endswith(b'is ringing'):
            chan, idx = delimited(line, b'-- ', b' ', pos)
            queue.ringing(line_no, when, chan)
            return

        # -- Told SIP/tk-0015b in group1 their queue position (which was 1)
        idx = line.find(b'Told', pos)
        if idx > 0:
            chan, idx = delimited(line, b' ', b' ', idx + 4)
            position, idx = delimited(line, b'which was ', b')', idx)
            queue.position(line_no, when, chan, position)
            return

        # -- SIP/320-0015a answered SIP/tk-0015b
        idx = line.find(b'answered', pos)
        if idx > 0:
            chan1 = line[idx + 9:]
            ans_by_chan, idx = delimited(line, b'-- ', b' ', pos)
            # Handle pickup (complicated!)
            if ans_by_chan in self.pickup_chans:
                pick_line_no, pick_line, pick_when, pick_chan = \
                    self.pickup_chans[ans_by_chan]
                queue.pickup(pick_line_no, pick_when, pick_chan, ans_by_chan)
                queue.channel.lines.append((pick_line_no, pick_line))
                # We don't have linked SIP because there is no hard link.
                # Attempt to link by finding interesting SIP messages
                # between pickup line number and answer line number
                phone = channel_phone(ans_by_chan)
                sip = self.find_ok_sip_from(phone, pick_line_no, line_no)
                if sip:
                    queue.channel.sip_set.add(sip)
            queue.answered(line_no, when, chan1, ans_by_chan)
            return

    def parse_manager_c(self, line_no, line, pos, when, acall):
        # -- Manager 'account' from 127.0.0.1, hanging up channel: SIP/301-0015a
        idx = line.find(b'hanging up channel: ', pos)
        if idx > 0:
            chan = line[idx + 20:]
            channel = self.channels.get(chan)
            if channel:
                channel.lines.append((line_no, line))
                if channel.current_dial:
                    channel.current_dial.manager_hangup(line_no, when, chan)
            return

    def link_acall(self, line_no, line, pos, when):
        l = 8
        idx = line.find(b'VERBOSE[', pos)
        if idx == -1:
            idx = line.find(b'WARNING[', pos)
            l = 8
            if idx == -1:
                idx = line.find(b'ERROR[', pos)
                l = 6
        if idx >= 0:
            pos = idx + l
            idx = line.find(b'][', pos, pos + 10)
            if idx > 0:
                pos = idx + 2
                idx = line.find(b']', pos, pos + 15)
                if idx > 0:
                    acall_id = line[pos:idx]
                    acall = self.acalls.get(acall_id)
                    if not acall:
                        acall = LogAstCall(acall_id, line_no, when)
                        self.acalls[acall_id] = acall
                    pos = idx + 1
                    acall.lines.append((line_no, line))
                    return acall, pos

        return None, pos

    def link_chan(self, line_no, line, chan, acall, when):
        if chan:
            channel = self.channels.get(chan)
            if not channel:
                channel = LogChannel(chan, line_no, when)
                self.channels[chan] = channel
            channel.lines.append((line_no, line))
            if acall:
                acall.channel_set.add(channel)
                channel.add_acall(acall)
            return channel

    def link_call(self, li, line, call_id, acall):
        if call_id:
            self.call_lines.setdefault(call_id, []).append((li, line))
            if acall:
                acall.call_id_set.add(call_id)
                self.call_acall_map.setdefault(call_id, set()).add(acall)

    def find_ok_sip_from(self, from_num, start_line_no, end_line_no):
        sips = self.sip_messages
        for i in range(len(sips) - 1, -1, -1):
            sip = sips[i]
            if sip.line_no < start_line_no:
                return None
            elif sip.line_no > end_line_no:
                continue
            if sip.from_num == from_num and sip.status == b'200 OK':
                return sip

    def find_sip_by_ref(self, sip_ref):
        call_id, dummy, line_no_txt = sip_ref.rpartition(b'/')
        sips = self.call_sip_map.get(call_id, [])
        for sip in sips:
            if str(sip.line_no + 1) == line_no_txt:
                return sip

    def retransmission_timeout(self, call_id, line_no, when):
        if call_id:
            self.call_timeouts[call_id] = (line_no, when)

    def get_phone_set(self):
        return set(self.phone_channel_map.keys()) | \
               set(self.phone_sip_map.keys()) | \
               set(self.queues.keys())

    def find_obj(self, ref_type, ref):
        if ref_type == 'call_id':
            return self.dialogs.get(ref)
        elif ref_type == 'sip_ref':
            return self.find_sip_by_ref(ref)
        elif ref_type == 'chan':
            return self.channels.get(ref)
        elif ref_type == 'acall_id':
            return self.acalls.get(ref)

    def search(self, number, chan, call_id):
        results = []
        channel = self.channels.get(chan)
        if channel:
            results.append(chan)
        sips = self.call_sip_map.get(call_id)
        if sips:
            results.append(call_id)
        results.extend(sorted(phone for phone
                              in self.get_phone_set()
                              if number in phone))
        return results

    def get_linked_objects(self, ref, isolate=(None, None), max_depth=10):
        log.debug('get linked objects ref=%s, isolate=%s, max_depth=%s',
                  ref, isolate, max_depth)
        groups = []

        mark_call_set = set()
        mark_sip_set = set()
        mark_acall_set = set()
        mark_channel_set = set()

        def include_dialog_sips(sips):
            all_sips = set(sips)
            for sip in sips:
                if sip.dialog:
                    all_sips |= set(sip.dialog.sip_list)
            return all_sips

        def add_sip(sip, level):
            if not sip or sip in mark_sip_set or level > max_depth:
                return
            mark_sip_set.add(sip)

            groups[-1].line(sip.line_no, 'sip', sip)

            if sip.call_id not in mark_call_set:
                mark_call_set.add(sip.call_id)
                s = sip.dialog.start_sip if sip.dialog else sip
                groups[-1].append(s.line_no, 'dialog', s)
                for line_no, line in self.call_lines.get(sip.call_id, []):
                    groups[-1].line(line_no, 'verbose', line)

            add_acall(sip.acall, level + 1)
            acall_set = self.call_acall_map.get(sip.call_id, set())
            for acall in acall_set:
                add_acall(acall, level + 1)

            for sip in include_dialog_sips([sip]):
                add_sip(sip, level + 1)

        def add_acall(acall, level):
            if not acall or acall in mark_acall_set or level > max_depth:
                return
            mark_acall_set.add(acall)

            min_line_no = None
            for line_no, line in acall.lines:
                groups[-1].line(line_no, 'verbose', line)
                if min_line_no is None or line_no < min_line_no:
                    min_line_no = line_no

            if min_line_no is not None:
                groups[-1].append(min_line_no, 'astcall', acall)

            for channel in acall.channel_set:
                add_channel(channel, level + 1)

            sip_set = acall.sip_set.copy()
            for call_id in acall.call_id_set:
                sip_set |= set(self.call_sip_map.get(call_id, []))
            sip_list = list(include_dialog_sips(sip_set))
            sip_list.sort(key=lambda s: s.line_no)
            for sip in sip_list:
                add_sip(sip, level + 1)

        def add_channel(channel, level):
            if not channel or channel in mark_channel_set or level > max_depth:
                return
            mark_channel_set.add(channel)

            min_line_no = None
            for line_no, line in channel.lines:
                groups[-1].line(line_no, 'channel', line)
                if min_line_no is None or line_no < min_line_no:
                    min_line_no = line_no
            if min_line_no is not None:
                groups[-1].append(min_line_no, 'channel', channel)

            for acall in channel.acall_set:
                add_acall(acall, level + 1)

            for sip in sorted(list(channel.sip_set), key=lambda s: s.line_no):
                add_sip(sip, level + 1)

        def link_all():
            sip_list = self.phone_sip_map.get(ref)
            if sip_list:
                log.debug('  found %d sip messages', len(sip_list))
                sip_list = list(include_dialog_sips(sip_list))
                log.debug('  extended to %d sip messages', len(sip_list))
                sip_list.sort(key=lambda s: s.line_no)
                for sip in sip_list:
                    groups.append(LogGroup())
                    add_sip(sip, 0)

            channels = self.phone_channel_map.get(ref)
            if channels:
                channels = sorted(list(channels),
                                  key=lambda c: (c.when, c.name))
                for channel in channels:
                    groups.append(LogGroup())
                    add_channel(channel, 0)

            queues = self.queues.get(ref)
            if queues:
                for queue in queues:
                    groups.append(LogGroup())
                    add_channel(queue.channel, 0)

            channel = self.channels.get(ref)
            if channel:
                groups.append(LogGroup())
                add_channel(channel, 0)

            for sip in self.call_sip_map.get(ref, []):
                groups.append(LogGroup())
                add_sip(sip, 0)
                break

        def isolate_groups(all_groups):
            all_groups = [g for g in all_groups if g.overview]
            for g in all_groups:
                g.overview.sort()
            all_groups.sort(key=lambda gr: gr.overview[0][0])

            if isolate and isolate != (None, None):
                ref_type, obj_ref = isolate
                find, obj = None, None
                if ref_type == 'call_id':
                    find = 'dialog'
                    dialog = self.dialogs.get(obj_ref)
                    obj = dialog and dialog.start_sip
                elif ref_type == 'sip_ref':
                    find = 'dialog'
                    sip = self.find_sip_by_ref(obj_ref)
                    obj = sip.dialog.start_sip if sip.dialog else sip
                elif ref_type == 'chan':
                    find = 'channel'
                    obj = self.channels.get(obj_ref)
                elif ref_type == 'acall_id':
                    find = 'astcall'
                    obj = self.acalls.get(obj_ref)

                if obj:
                    for g in all_groups:
                        for l, w, o in g.overview:
                            if w == find and o == obj:
                                return [g]
                    return []
            return all_groups

        def get_objects(groups):
            objs = {}
            for g in groups:
                for lno, (style, line) in g.lines.items():
                    objs[lno] = (style, line)
            return objs

        link_all()
        groups = isolate_groups(groups)
        objects = get_objects(groups)

        return groups, objects


class LogGroup(object):
    def __init__(self):
        self.overview = []
        self.lines = {}

    def append(self, line_no, kind, obj):
        self.overview.append((line_no, kind, obj))

    def line(self, line_no, style, line):
        if style == 'verbose' and line_no in self.lines:
            return
        self.lines[line_no] = (style, line)

    def sort(self):
        self.overview.sort()


def find_file_position(f, when, direction='after', is_cdr=False):
    ts = parse_when(when)
    if not ts:
        return None

    f.seek(0, os.SEEK_END)
    size = f.tell()

    file_pos, a, b = 0, 0, size
    good_pos = None
    for cnt in range(40):
        new_file_pos = a + (b - a) / 2
        if new_file_pos == file_pos:
            break
        file_pos = new_file_pos
        f.seek(file_pos, os.SEEK_SET)
        data = f.read(64000)
        w, pos = read_cdr_when(data) if is_cdr else read_when(data)
        if not w:
            break
        t = parse_when(w)
        if not t:
            break
        if direction == 'after':
            if t >= ts:
                good_pos = file_pos + pos
                b = file_pos
            else:
                a = file_pos
        else:
            if t <= ts:
                good_pos = file_pos + pos
                a = file_pos
            else:
                b = file_pos
        if b == a:
            break

    return good_pos


def read_when(data):
    if not data:
        return None, None
    data_pos = 0
    data_len = len(data)
    while data_pos < data_len:
        line_pos = data_pos
        line, data_pos = get_line(data, data_pos, data_len)
        if line and line[0] == b'[':
            idx = line.find(b']')
            if idx > 0:
                return line[1:idx], line_pos
    return None, None


def read_cdr_when(data):
    if not data:
        return None, None
    data_pos = 0
    data_len = len(data)
    while data_pos < data_len:
        line_pos = data_pos
        line, data_pos = get_line(data, data_pos, data_len)
        if line:
            rdr = csv.reader(StringIO(line))
            row = rdr.next()
            if len(row) >= 16:
                start_when = row[9]
                end_when = row[10]
                start_ts = parse_when(start_when)
                end_ts = parse_when(end_when)
                if start_ts and end_ts:
                    return start_when, line_pos
    return None, None


def parse_from_to(line, start):
    if line[start] == b'<':
        num, pos = delimited(line, b'<sip:', b'>', start)
        name = None
    elif line[start] == b'"':
        name, pos = delimited(line, b'"', b'"', start)
        num, pos = delimited(line, b'<sip:', b'>', pos)
    else:
        num, pos = delimited(line, b'<sip:', b'>', start)
        name, pos = delimited(line, b' ', b' ', start, pos)
    addr = None
    if num:
        pos = num.find(b';')
        if pos > 0:
            num = num[:pos]
        pos = num.find(b'@')
        if pos > 0:
            original = num
            num = original[:pos]
            addr = original[pos + 1:]
            if b':' not in addr:
                addr += b':5060'
    return name, num, addr


def delimited(line, delimiter, delimiter2, start, end=None, rest=False):
    a = line.find(delimiter, start, end)
    if a >= 0:
        ld = len(delimiter)
        b = line.find(delimiter2, a + ld)
        if b >= 0:
            return line[a + ld:b], b
        elif rest:
            return line[a + ld:], len(line)
    return None, None


def get_line(data, data_pos, data_len):
    eol = data.find(b'\n', data_pos)
    if eol == -1:
        # line = data[data_pos:]
        line = ''  # We are not interested in lines without NL (half lines)
        data_pos = data_len
    elif data[eol - 1] == b'\r':
        line = data[data_pos:eol - 1]
        data_pos = eol + 1
    else:
        line = data[data_pos:eol]
        data_pos = eol + 1

    return line, data_pos


def device_phone(device_name):
    idx = device_name.rfind(b'/')
    if idx > 0:
        device_name = device_name[idx + 1:]
    return device_name


def channel_phone(channel_name):
    idx = channel_name.rfind(b'/')
    if idx > 0:
        idx2 = channel_name.find(b'-', idx)
        if idx2 > 0:
            channel_name = channel_name[idx + 1:idx2]
        else:
            channel_name = channel_name[idx + 1:]
    return channel_name


def get_memory_size():
    with open('/proc/meminfo') as f:
        meminfo = f.read()
    m = re.search(r'^MemTotal:\s+(\d+)', meminfo)
    if m:
        mem_total_kb = int(m.groups()[0])
        return mem_total_kb * 1024


if __name__ == '__main__':
    import sys


    def main(log_file, call_id):
        parser = LogParser(log_file)
        parser.load_file()

        # import pdb
        # import gc
        # gc.collect()
        # pdb.set_trace()

        sips = parser.call_sip_map.get(call_id)
        if sips:
            # print len(sips), sips
            # print len(sips[0].dialog.sip_list), sips[0].dialog.sip_list
            assert sips == sips[0].dialog.sip_list
            for i, sip in enumerate(sips):
                if i:
                    sys.stdout.write('\n')
                    sys.stdout.write('-----------------------~n')

                sender = sip.sender_addr or ''
                if ':' not in sender:
                    sender += ':5060'
                recipient = sip.recipient_addr or ''
                if ':' not in recipient:
                    recipient += ':5060'
                    sys.stdout.write('[%s] %s %s --> %s\n' % (
                        sip.when.replace('-', '/'),
                        sip.direction,
                        sender, recipient))
                # print 'via:%s, to:%s, from:%s, req:%s' % (sip.via_addr,
                # sip.to_addr, sip.from_addr, sip.request_addr)
                sys.stdout.write('\n')
                for line in sip.header:
                    sys.stdout.write(line)
                    sys.stdout.write('\n')
                if sip.body:
                    sys.stdout.write('\n')
                    for line in sip.body:
                        sys.stdout.write(line)
                        sys.stdout.write('\n')
            sys.stdout.flush()


    main(sys.argv[1], sys.argv[2])
