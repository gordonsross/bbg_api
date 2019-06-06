from collections import OrderedDict
from datetime import datetime

import blpapi
from blpapi import DataType
from blpapi import NotFoundException, UnknownErrorException


SECURITY_DATA = blpapi.Name('securityData')
SECURITY = blpapi.Name('security')
BAR_DATA = blpapi.Name('barData')
BAR_TICK_DATA = blpapi.Name('barTickData')
FIELD_DATA = blpapi.Name('fieldData')
FIELD_EXCEPTIONS = blpapi.Name('fieldExceptions')
SECURITY_ERROR = blpapi.Name('securityError')
RESPONSE_ERROR = blpapi.Name('responseError')
FIELD_ID = blpapi.Name('fieldId')
ERROR_INFO = blpapi.Name('errorInfo')
CATEGORY = blpapi.Name('category')

BBG_SEQUENCES = [DataType.SEQUENCE, DataType.BYTEARRAY, DataType.CHOICE]


class RequestType(object):
    HISTORICAL_DATA = 'HistoricalDataRequest'
    REFERENCE_DATA = 'ReferenceDataRequest'
    INTRADAY_BAR_DATA = 'IntradayBarRequest'


class ServiceType(object):
    DEFAULT_SVC = '//blp/refdata'
    API_FIELDS = '//blp/apiflds'
    TECHNICAL_ANALYSIS = '//blp/tasvc'
    API_AUTHORISATION = '//blp/apiauth'


class IntradayBarRequestEventType(object):
    TRADE = 'TRADE'
    BID = 'BID'
    ASK = 'ASK'
    BEST_BID = 'BEST_BID'
    BEST_ASK = 'BEST_ASK'


class Periodicity(object):
    DAILY = 'DAILY'
    WEEKLY = 'WEEKLY'
    MONTHLY = 'MONTHLY'
    QUARTERLY = 'QUARTERLY'
    SEMI_ANNUAL = 'SEMI-ANNUAL'
    YEARLY = 'YEARLY'


class NonTradingDayFillOption(object):
    NON_TRADING_WEEKDAYS = 'NON_TRADING_WEEKDAYS'
    ALL_CALENDAR_DAYS = 'ALL_CALENDAR_DAYS'
    ACTIVE_DAYS_ONLY = 'ACTIVE_DAYS_ONLY'


class NonTradingDayFillMethod(object):
    PREVIOUS_VALUE = 'PREVIOUS_VALUE'
    NIL_VALUE = 'NIL_VALUE'


class ConnectionFailed(Exception):
    def __init__(self, message):
        super(ConnectionFailed, self).__init__(message)


class Session(object):

    DEFAULT_HOST = 'localhost'
    DEFAULT_PORT = 8194

    def __init__(self, host=None, port=None):
        """
        :param host: e.g. localhost
        :type host: str
        :param port: e.g. 8194
        :type port: int
        """
        self.host = self.DEFAULT_HOST or host
        self.port = self.DEFAULT_PORT or port
        self.session_options = blpapi.SessionOptions()
        self.session_options.setServerHost(self.host)
        self.session_options.setServerPort(self.port)
        self.session = blpapi.Session(self.session_options)
        return

    def start(self):
        if not self.session.start():
            raise ConnectionFailed('Connection Failed')
        return self.session


class Request(object):

    def __init__(self, session, request_type, service_type=ServiceType.DEFAULT_SVC):
        self.session = session
        self.request_type = request_type
        self.service_name = service_type
        self.request = None
        return

    def create(self):

        try:
            self.session.openService(self.service_name)
            ref_data_service = self.session.getService(self.service_name)
            self.request = ref_data_service.createRequest(self.request_type)
            return self.request
        except NotFoundException:
            raise
        except UnknownErrorException:
            raise


class SessionFactory(object):

    def __init__(self, session, cid, request_type):
        self.session = session
        self.cid = cid
        self.request_type = request_type
        return

    def process_session(self):

        output = {}
        security_responses = {}

        msg_processes = {RequestType.HISTORICAL_DATA: self._process_historical_data_msg,
                         RequestType.REFERENCE_DATA: self._process_ref_data_msg,
                         RequestType.INTRADAY_BAR_DATA: self._process_intraday_data_msg}

        while True:
            ev = self.session.nextEvent(500)

            for msg in ev:

                if self.cid in msg.correlationIds():

                    security, sec_results = msg_processes[self.request_type](msg)
                    response_type = str(msg.messageType())

                    # For large requests (particularly intraday), the BBG message will be split into several responses.
                    # Each response must be appended to the output.
                    if security not in security_responses:
                        security_responses[security] = []
                        security_responses[security].append(response_type)
                    else:
                        security_responses[security].append(response_type)

                    msg_response_count = self._get_response_count(security, security_responses, response_type)

                    if security:
                        if msg_response_count > 1:
                            output[security] = self._merge_two_dicts(output[security], sec_results)
                        else:
                            output[security] = sec_results
                    else:
                        if msg_response_count > 1:
                            output = self._merge_two_dicts(output, sec_results)
                        else:
                            output = sec_results

            if ev.eventType() == blpapi.Event.RESPONSE:
                break

        return output

    def _get_response_count(self, security, security_responses, request_type):
        return security_responses[security].count(request_type)

    def _process_intraday_data_msg(self, msg):

        results = OrderedDict()

        # Check for response errors
        if msg.hasElement(RESPONSE_ERROR):
            return None, self._response_error_factory(msg.getElement(RESPONSE_ERROR))

        # Make sure there is bar data and get it
        if not msg.hasElement(BAR_DATA):
            raise NotFoundException('No Bar Data in message.', 666)
        bar_data = msg.getElement(BAR_DATA)

        for idx, bar_tick_data in enumerate(bar_data.getElement(BAR_TICK_DATA).values()):

            data = {}

            for element in bar_tick_data.elements():
                data.setdefault(str(element.name()), element.getValue())

            results[data['time']] = data

        return None, results

    def _process_ref_data_msg(self, msg):
        """
        Processing for Reference Data calls
        :param msg: Message from from Bloomberg Event
        :type msg:
        :return:
        :rtype:
        """

        results = {}

        # Check for response errors
        if msg.hasElement(RESPONSE_ERROR):
            return None, self._response_error_factory(msg.getElement(RESPONSE_ERROR))

        # Check to make sure there is security data present
        if not msg.hasElement(SECURITY_DATA):
            raise NotFoundException('No Security Data in message.', 666)

        # Get security data and name
        security_data = msg.getElement(SECURITY_DATA)
        security = None

        # Get generic iterator for data and find its length
        security_iterator = self._get_bbg_iterator(security_data)
        security_iterator_len = self._get_iterator_len(security_data)

        for item in security_iterator:

            security = item.getElementAsString(SECURITY)

            # Check for field data
            field_data = item.getElement(FIELD_DATA)
            if self._get_iterator_len(field_data) > 0:
                security_data_results = self._flatten_dict(self._element_factory(field_data, []))
            else:
                security_data_results = {}

            # Check for errors with the ticker
            security_errors = {}
            if item.hasElement(SECURITY_ERROR):
                security_errors = self._security_error_factory(item.getElement(SECURITY_ERROR))

            # Check for errors with fields
            field_exceptions = {}
            if self._get_iterator_len(item.getElement(FIELD_EXCEPTIONS)) > 0:
                field_exceptions = self._field_exception_factory(item.getElement(FIELD_EXCEPTIONS))

            if security_iterator_len > 1:   # For nested results e.g. Yield Curve Objects
                results[security] = {'security_data': security_data_results, 'field_exceptions': field_exceptions,
                                     'security_errors': security_errors}
            else:   # For flat results, e.g. single Equity, Future or Bond tickers
                results = {'security_data': security_data_results, 'field_exceptions': field_exceptions,
                           'security_errors': security_errors}

        if security_iterator_len > 1:
            return None, results
        else:
            return security, results

    def _process_historical_data_msg(self, msg):

            # Check for response errors
            if msg.hasElement(RESPONSE_ERROR):
                return None, self._response_error_factory(msg.getElement(RESPONSE_ERROR))

            # Check to make sure there is security data present
            if not msg.hasElement(SECURITY_DATA):
                raise NotFoundException('No Security Data in message.', 666)

            # Get security data and name
            security_data = msg.getElement(SECURITY_DATA)
            security = str(security_data.getElement(SECURITY).getValue())

            # Check for security errors, e.g. invalid ticker
            if security_data.hasElement(SECURITY_ERROR):
                return security, {'security_errors': self._security_error_factory(security_data.getElement(SECURITY_ERROR)),
                                  'security_data': {},
                                  'field_exceptions': {}}

            # Check for invalid field identifiers
            field_exceptions = {}
            if security_data.hasElement(FIELD_EXCEPTIONS):
                field_exceptions = self._field_exception_factory(security_data.getElement(FIELD_EXCEPTIONS))

            # Get the field data for the security
            field_data = security_data.getElement(FIELD_DATA)

            if self._get_iterator_len(field_data) > 0:
                security_data_results = {}

                for item in self._get_bbg_iterator(field_data):
                    item_data = {}
                    date = None

                    for element in self._get_bbg_iterator(item):
                        measure = str(element.name())
                        value = element.getValue()

                        if measure == 'date':
                            date = value
                            security_data_results.setdefault(date, {})
                        else:
                            item_data[measure] = value

                    security_data_results[date] = item_data
            else:
                security_data_results = {}

            results = {'security_data': security_data_results, 'security_errors': {}, 'field_exceptions': field_exceptions}

            return security, results

    def _response_error_factory(self, response_array):
        response_errors = {}

        for response_error in self._get_bbg_iterator(response_array):
            response_errors[str(response_error.name())] = response_error.getValue()

        return response_errors

    def _field_exception_factory(self, field_exception_array):
        field_exceptions = {}

        for field_exception in self._get_bbg_iterator(field_exception_array):
            error_info = field_exception.getElement(ERROR_INFO)
            field_exceptions[error_info.getElementAsString(CATEGORY)] = \
                field_exception.getElementAsString(FIELD_ID)

        return field_exceptions

    def _security_error_factory(self, security_error):
        security_errors = {}

        for error in self._get_bbg_iterator(security_error):
            security_errors[str(error.name())] = error.getValue()

        return security_errors

    def _element_factory(self, element, duplicate_elements, results=None):

        if not element.isValid():
            raise blpapi.NotFoundException

        data_type = element.datatype()
        element_name = str(element.name())

        if data_type in BBG_SEQUENCES:

            if results is None:
                results = {}

            if element_name not in results:
                results[element_name] = {}
            else:
                element_name = '{0}_{1}'.format(element_name, str(len(duplicate_elements)))
                duplicate_elements.append(element_name)
                results[element_name] = {}

            for sub_element in self._get_bbg_iterator(element):
                self._element_factory(sub_element, duplicate_elements, results[element_name])

        else:
            single_value = self._field_factory(element)
            results[single_value[0]] = single_value[1]
            return results

        return results

    def _field_factory(self, data):

        dtf = DataTypeFactory()

        field_data_type = data.datatype()

        process_factory = {DataType.BOOL: dtf.process_bool,
                           DataType.BYTE: dtf.process_byte,
                           DataType.BYTEARRAY: dtf.process_byte_array,
                           DataType.CHAR: dtf.process_char,
                           DataType.CORRELATION_ID: dtf.process_correlation_id,
                           DataType.DATE: dtf.process_date,
                           DataType.DATETIME: dtf.process_date_time,
                           DataType.DECIMAL: dtf.process_scalar,
                           DataType.ENUMERATION: dtf.process_enumeration,
                           DataType.FLOAT32: dtf.process_scalar,
                           DataType.FLOAT64: dtf.process_scalar,
                           DataType.INT32: dtf.process_scalar,
                           DataType.INT64: dtf.process_scalar,
                           DataType.STRING: dtf.process_string,
                           DataType.TIME: dtf.process_time}
        try:
            single_val_results = process_factory.get(field_data_type)(data)
            return [single_val_results[0], single_val_results[1]]
        except Exception:
            raise

    def _get_bbg_iterator(self, obj):
        if obj.isArray():
            return obj.values()
        if obj.isComplexType():
            return obj.elements()

    def _get_iterator_len(self, obj):
        if obj.isArray():
            return obj.numValues()
        if obj.isComplexType():
            return obj.numElements()

    def _merge_two_dicts(self, x, y):
        # Given two dicts, merge them into a new dict as a shallow copy.
        z = x.copy()
        z.update(y)
        return z

    def _flatten_dict(self, obj):

        if not isinstance(obj, dict):
            return obj
        elif len(obj) > 1:
            return obj
        else:
            for k, v in obj.items():
                if isinstance(v, dict) and len(v) == 1:
                    return v
                else:
                    return self._flatten_dict(v)

class DataTypeFactory(object):

    def __init__(self):
        return

    def process_bool(self, data):
        return [str(data.name()), data.getValue()]

    def process_byte(self, data):
        return [str(data.name()), data.getValue()]

    def process_byte_array(self, data):
        return [str(data.name()), data.getValue()]

    def process_char(self, data):
        return [str(data.name()), data.getValue()]

    def process_correlation_id(self, data):
        return NotImplementedError

    def process_date(self, data):
        return [str(data.name()), data.getValue().strftime('%Y-%m-%d')]

    def process_date_time(self, data):
        return [str(data.name()), data.getValue().strftime('%Y-%m-%d %H:%M:%S')]

    def process_enumeration(self, data):
        return NotImplementedError

    def process_string(self, data):
        return [str(data.name()), data.getValue()]

    def process_time(self, data):
        return [str(data.name()), data.getValue().strftime('%H:%M:%S')]

    def process_scalar(self, data):
        return [str(data.name()), data.getValue()]


class IntradayBarRequest(object):

    def __init__(self):
        return

    @staticmethod
    def get(ticker, event_type, interval, start_date, end_date, max_data_points=2500):
        """
        Intraday Bar Request for Bloomberg Data
        :param ticker:  Single Bloomberg Ticker e.g. IBM US Equity
        :type ticker: str
        :param event_type: One of 'TRADE', 'BID', 'ASK', 'BEST_BID', 'BEST_ASK' or use IntradayBarRequestEventType
        :type event_type: str
        :param interval: Time period interval, e.g. 30
        :type interval: int
        :param start_date: Python Date or DateTime stamp
        :type start_date: datetime
        :param end_date: Python Date or DateTime stamp
        :type end_date: datetime
        :param max_data_points: Maximum number of points to return
        :type max_data_points: int
        :return: Dictionary of datetimes and open, high, low, close, volume
        :rtype: dict
        """

        try:
            assert isinstance(ticker)
            assert isinstance(interval, int)
            assert event_type in ('TRADE', 'BID', 'ASK', 'BEST_BID', 'BEST_ASK')
            assert isinstance(start_date, datetime)
            assert isinstance(end_date, datetime)
            assert isinstance(max_data_points, int)
        except AssertionError:
            raise

        try:
            session = Session().start()
            request = Request(session, RequestType.INTRADAY_BAR_DATA).create()

            request.set('security', ticker)
            request.set('eventType', event_type)
            request.set('interval', interval)
            request.set('startDateTime', start_date)
            request.set('endDateTime', end_date)
            request.set('maxDataPoints', max_data_points)

            cid = session.sendRequest(request)

        except:
            raise

        try:
            return SessionFactory(session, cid, RequestType.INTRADAY_BAR_DATA).process_session()
        except:
            raise
        finally:
            session.stop()


class HistoricalDataRequest(object):

    def __init__(self):
        return

    @staticmethod
    def get(tickers, fields, start_date, end_date, periodicity=Periodicity.DAILY, max_data_points=2500,
            non_trading_day_fill_option=NonTradingDayFillOption.ACTIVE_DAYS_ONLY,
            non_trading_day_fill_method=NonTradingDayFillMethod.NIL_VALUE

            ):
        """
        Historical Data Request for Bloomberg Data
        :param tickers: List of Bloomberg Ticker Strings
        :type tickers: list
        :param fields: List of Bloomberg Field Names
        :type fields: list
        :param start_date: Python Date stamp
        :type start_date: datetime
        :param end_date: Python Date stamp
        :type end_date: datetime
        :param periodicity: One of 'DAILY', 'WEEKLY', 'MONTHLY', 'QUARTERLY', 'SEMI-ANNUAL', 'YEARLY'. There is a helper
        class 'Periodicity' you can use to enter periodicity.
        :type periodicity: str
        :param max_data_points: Maximum number of points to return
        :type max_data_points: int
        :param non_trading_day_fill_option: One of 'NON_TRADING_WEEKDAYS', 'ALL_CALENDAR_DAYS', 'ACTIVE_DAYS_ONLY'
        :type non_trading_day_fill_option: str
        :param non_trading_day_fill_method: Either 'PREVIOUS_VALUE' or 'NIL_VALUE'
        :type non_trading_day_fill_method: str
        :return: Dictionary of Tickers, dates and fields
        :rtype: dict
        """

        try:
            assert isinstance(tickers, list)
            assert isinstance(fields, list)
            assert periodicity in ('DAILY', 'WEEKLY', 'MONTHLY', 'QUARTERLY', 'SEMI-ANNUAL', 'YEARLY')
            assert isinstance(start_date, datetime)
            assert isinstance(end_date, datetime)
            assert isinstance(max_data_points, int)
            assert non_trading_day_fill_option in ('NON_TRADING_WEEKDAYS', 'ALL_CALENDAR_DAYS', 'ACTIVE_DAYS_ONLY')
            assert non_trading_day_fill_method in ('PREVIOUS_VALUE', 'NIL_VALUE')

        except AssertionError:
            raise

        start_date = start_date.strftime('%Y%m%d')
        end_date = end_date.strftime('%Y%m%d')

        try:
            session = Session().start()
            request = Request(session, RequestType.HISTORICAL_DATA).create()

            for ticker in tickers:
                request.append('securities', ticker)
            for field in fields:
                request.append('fields', field)

            request.set('periodicityAdjustment', 'ACTUAL')
            request.set('periodicitySelection', periodicity)
            request.set('startDate', start_date)
            request.set('endDate', end_date)
            request.set('maxDataPoints', max_data_points)
            request.set('nonTradingDayFillOption', non_trading_day_fill_option)
            request.set('nonTradingDayFillMethod', non_trading_day_fill_method)

            cid = session.sendRequest(request)

        except:
            raise

        try:
            return SessionFactory(session, cid, RequestType.HISTORICAL_DATA).process_session()
        except:
            raise
        finally:
            session.stop()


class ReferenceDataRequest(object):

    def __init__(self):
        return

    @staticmethod
    def get(tickers, fields, overrides=None):
        """
        Reference Data Request i.e. non Market, Static Data for Bloomberg
        :param tickers: List of Bloomberg Ticker Strings
        :type tickers: list
        :param fields: List of Bloomberg Field Names
        :type fields: list
        :param overrides: Dictionary of Field & Field Value overrides
        :type overrides: dict
        :return: Dictionary of Tickers & return fields
        :rtype: dict
        """
        try:
            assert isinstance(tickers, list)
            assert isinstance(fields, list)
            if overrides:
                assert isinstance(overrides, dict)
        except AssertionError:
            raise

        try:
            session = Session().start()
            request = Request(session, RequestType.REFERENCE_DATA).create()

            for ticker in tickers:
                request.append('securities', ticker)
            for field in fields:
                request.append('fields', field)

            if overrides:
                for field_id, value in overrides.items():
                    override = request.getElement('overrides').appendElement()
                    override.setElement('fieldId', field_id)
                    override.setElement('value', ReferenceDataRequest.format_override(field_id, value))

            cid = session.sendRequest(request)

        except:
            raise

        try:
            return SessionFactory(session, cid, RequestType.REFERENCE_DATA).process_session()
        except:
            raise
        finally:
            session.stop()

    @staticmethod
    def format_override(field_id, value):
        if field_id == 'CURVE_DATE':
            return value.strftime('%Y%m%d')

