import sys
import csv
from collections import OrderedDict
from datetime import datetime
from datetime import timedelta

_format_ = '%Y-%m-%d %H:%M:%S'


# get time different between dt1 and dt2 in seconds
def get_time_diff(dt_first, dt_last):
    dt1 = datetime.strptime(dt_first, _format_)
    dt2 = datetime.strptime(dt_last, _format_)
    return int((dt2 - dt1).total_seconds())


# write clean log into sessionization.txt
def write(f_writer, o_session):
    f_writer.write(
        '{},{},{},{},{}\n'.format(o_session.ip, o_session.first_dt, o_session.last_dt, o_session.get_duration_time(),
                                  o_session.document_count))


class Session:

    # ip = 0  ## IP address of the user exactly as found in log1.csv
    # first_time = ''  ## date and time of the first webpage request in the session (yyyy-mm-dd hh:mm:ss)
    # last_time = ''  ## date and time of the last webpage request in the session (yyyy-mm-dd hh:mm:ss)
    # session_duration = 0  ## duration of the session in seconds
    # document_count = 0  ## the combination of cik, accession and extention fields

    def __init__(self, s_ip, s_datetime, s_document):
        self.ip = s_ip
        self.first_dt = s_datetime
        self.last_dt = s_datetime
        self.documents = [s_document]

        self.session_duration = 1
        self.document_count = 1

    def get_duration_time(self):
        self.session_duration = get_time_diff(self.first_dt, self.last_dt) + 1
        return self.session_duration

    def update_session(self, s_datetime, s_document):
        self.last_dt = s_datetime
        self.documents.append(s_document)
        self.document_count = len(self.documents)


# Gather our code in a process() function
def process():
    input_file_path = sys.argv[1]  # set log1.csv file path
    parameter_file_path = sys.argv[2]  # set inactivity_period.txt file path
    output_file_path = sys.argv[3]  # set sessionization.txt file path

    ip_session_map = OrderedDict()  # hashmap ip as key and session as value
    dt_ips_map = OrderedDict()  # hashmap datetime as key and ips as value
    first_log = True  # identifies first log in weblogs
    last_processed_dt = ''  # last processed datetime

    # read inactivity_period.txt file
    with open(parameter_file_path, 'r') as parameter_file:
        # the period of inactivity (in seconds) that your program should use to identify a user session
        inactivity_period = int(parameter_file.read())

    # open log1.csv and sessionization.txt
    with open(input_file_path, 'r') as input_file, open(output_file_path, 'w') as output_file:
        _reader_ = csv.reader(input_file)

        # get indices from header
        _header = _reader_.next()
        _ip = _header.index('ip')
        _date = _header.index('date')
        _time = _header.index('time')
        _cik = _header.index('cik')
        _accession = _header.index('accession')
        _extention = _header.index('extention')

        # streaming data by reading log1.csv line-by-line
        for _log_ in _reader_:
            _ip_ = _log_[_ip]
            _date_ = _log_[_date]
            _time_ = _log_[_time]
            _cik_ = _log_[_cik]
            _accession_ = _log_[_accession]
            _extention_ = _log_[_extention]
            _document_ = _cik_ + '-' + _accession_ + _extention_

            current_str_dt = _date_ + ' ' + _time_

            if first_log:
                last_processed_dt = current_str_dt
                first_log = False

            # check if ip already existed
            if _ip_ not in ip_session_map:
                # instantiate new session
                ip_session_map[_ip_] = Session(_ip_, current_str_dt, _document_)
            else:
                # update last_time and document_count in current session object
                ip_session_map[_ip_].update_session(current_str_dt, _document_)

            current_dt = datetime.strptime(current_str_dt, _format_)
            if current_dt not in dt_ips_map:
                dt_ips_map[current_dt] = [_ip_]
            else:
                if _ip_ not in dt_ips_map[current_dt]:
                    dt_ips_map[current_dt].append(_ip_)

            # check if session is inactive, if so, write the log and remove it from ip_session_map
            if current_str_dt != last_processed_dt:
                last_dt = datetime.strptime(last_processed_dt, _format_) - timedelta(seconds=inactivity_period)
                if last_dt in dt_ips_map:
                    for i_ip in dt_ips_map[last_dt]:
                        if get_time_diff(ip_session_map[i_ip].last_dt, last_dt.strftime(_format_)) == 0:
                            write(output_file, ip_session_map[i_ip])
                            del ip_session_map[i_ip]
                    del dt_ips_map[last_dt]
                last_processed_dt = current_str_dt
                '''for i_ip in ip_session_map.keys():
                    if get_time_diff(ip_session_map[i_ip].last_dt, _datetime_) > inactivity_period:
                        i_session = ip_session_map[i_ip]
                        write(output_file, i_session)
                        del ip_session_map[i_ip]'''

        # wait until all session are done ## write remained active session in ip_session_map
        for r_ip in ip_session_map.keys():
            r_session = ip_session_map[r_ip]
            write(output_file, r_session)
            del ip_session_map[r_ip]


# Standard boilerplate to call the main() function to begin the program.
if __name__ == '__main__':
    process()
    print 'Done'
