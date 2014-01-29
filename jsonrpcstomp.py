import random
import json
import string
import time

from stompy.simple import Client
from stompy.stomp import ConnectionError

DEFAULT_PORT = 62623
DEFAULT_HOST = 'localhost'
DEFAULT_RETRY_TIME = 1 # in seconds

PREFIX_TOPIC_MESSAGE = '/topic/msg.local.'
PREFIX_TOPIC_JOB = '/topic/req.local.'
PREFIX_REPLY = '/topic/reply-'

JSONRPC_KEY_PARAMS = 'params'
JSONRPC_KEY_METHOD = 'method'
JSONRPC_KEY_ID = 'id'
JSONRPC_KEY_RESULT = 'result'
JSONRPC_KEY_JSONRP = 'jsonrpc'
JSONRPC_KEY_ERROR = 'error'
JSONRPC_KEY_VERSION = '2.0'
JSONRPC_KEY_REPLY_TO = 'reply-to'

JSONRPC_KEY_ERROR_CODE = 'code'
JSONRPC_KEY_ERROR_MESSAGE = 'message'

JSONRPC_VALUE_ERROR_PARSE = -32700
JSONRPC_VALUE_ERROR_IN_REQ = -32600
JSONRPC_VALUE_ERROR_MET_NOT = -32601
JSONRPC_VALUE_ERROR_INV_PAR = -32602
JSONRPC_VALUE_ERROR_INT_ERR = -32603

JSONRPC_VALUE_MSG_PARSE = 'Parse error'
JSONRPC_VALUE_MSG_IN_REQ = 'Invalid Request'
JSONRPC_VALUE_MSG_MET_NOT = 'Method not found'
JSONRPC_VALUE_MSG_INV_PAR = 'Invalid params'
JSONRPC_VALUE_MSG_INT_ERR = 'Internal error'

MSG_ERROR_METHOD_NOT_STR = 'The method must be a string'
MSG_ERROR_NOT_CONNECTED = 'The listener is not connected to an stomp server'
ACK_CLIENT = 'client'
ACK_AUTO = 'auto'

MSG_TYPE_JOB = 'job'
MSG_TYPE_NOT = 'notification'
MSG_TYPE_CALLBACK_SUCCESS = 'callback_sucess'
MSG_TYPE_CALLBACK_ERROR = 'callback_error'

MSG_QUIT = 'quit'

# TODO: Error handling for the jsonrpc protocol


class NotConnectedError(Exception):
    pass


class JSONRPCObject(object):
    """Class representing a JSONRPC message."""
    def __init__(self, message):
        destination = message.headers.get('destination')
        json_msg = json.loads(message.body)

        self.params = json_msg.get(JSONRPC_KEY_PARAMS)
        self.method = json_msg.get(JSONRPC_KEY_METHOD)
        self._id = json_msg.get(JSONRPC_KEY_ID)
        self.result = json_msg.get(JSONRPC_KEY_RESULT)
        self.error = json_msg.get(JSONRPC_KEY_ERROR)

        if self._id is None:
            self.message_type = MSG_TYPE_NOT
            self.notification = destination.partition(PREFIX_TOPIC_MESSAGE)[-1]
        else:
            if destination.startswith(PREFIX_TOPIC_JOB):
                self.message_type = MSG_TYPE_JOB
                self.method = destination.partition(PREFIX_TOPIC_JOB)[-1]
                self.reply_to = message.headers.get(JSONRPC_KEY_REPLY_TO)
            if destination.startswith(PREFIX_REPLY):
                if self.result is not None:
                    self.message_type = MSG_TYPE_CALLBACK_SUCCESS
                else:
                    self.message_type = MSG_TYPE_CALLBACK_ERROR

        super(JSONRPCObject, self).__init__()

    @property
    def is_notification(self):
        return True if self.message_type == MSG_TYPE_NOT else False

    @property
    def is_job(self):
        return True if self.message_type == MSG_TYPE_JOB else False

    @property
    def is_callback(self):
        return True if self.message_type == MSG_TYPE_CALLBACK_SUCCESS or \
            self.message_type == MSG_TYPE_CALLBACK_ERROR else False


class JSONRPCStomp(Client):
    """Class that inherits from stompy.Client and wrap
       methods to comply with JSONRPC standard."""

    def __init__(self, *args, **kwargs):
        kwargs['port'] = kwargs.get('port', DEFAULT_PORT)
        super(JSONRPCStomp, self).__init__(*args, **kwargs)
        self.methods = {}
        self.notifiers = {}
        self.callbacks_success = {}
        self.callbacks_error = {}
        self.is_connected = False
        self.last_id = 0

    def connect(self, *args, **kwargs):
        """Connect to stomp server and send a random reply topic."""
        is_connected = False

        while not is_connected:
            try:
                super(JSONRPCStomp, self).connect(*args, **kwargs)
            except ConnectionError:
                time.sleep(DEFAULT_RETRY_TIME)
            else:
                is_connected = True

        # This flag is only set if an exception is not raised
        # Create random string channel to receive responses
        random_string = ''.join(random.choice(string.ascii_uppercase +
            string.digits) for x in range(16))

        # Need to subscribe to a random channel to receive the responses
        # In case calls are made
        self.reply_to_topic = PREFIX_REPLY + random_string

        self.subscribe(self.reply_to_topic, ACK_CLIENT)

        self.is_connected = is_connected

    def disconnect(self, *args, **kwargs):
        """Disconnect from stomp server."""
        super(JSONRPCStomp, self).disconnect(*args, **kwargs)
        self.is_connected = False

    def accept_notifications(self, method, callback):
        """Register a notification."""
        if self.is_connected:
            if isinstance(method, str):
                self.notifiers[method] = callback
                method_name = PREFIX_TOPIC_MESSAGE + method
                self.subscribe(method_name, ACK_AUTO)
            else:
                raise TypeError(MSG_ERROR_METHOD_NOT_STR)
        else:
            raise NotConnectedError(MSG_ERROR_NOT_CONNECTED)

    def accept_calls(self, method, callback):
        """Register a call."""
        if self.is_connected:
            if isinstance(method, str):
                self.methods[method] = callback
                method_name = PREFIX_TOPIC_JOB + method
                self.subscribe(method_name, ACK_AUTO)
            else:
                raise TypeError(MSG_ERROR_METHOD_NOT_STR)
        else:
            raise NotConnectedError(MSG_ERROR_NOT_CONNECTED)

    def notify(self, method, params):
        """Send remote notification."""

        message = {JSONRPC_KEY_JSONRP: JSONRPC_KEY_VERSION,
                JSONRPC_KEY_METHOD: method,
                JSONRPC_KEY_PARAMS: params}

        json_message = json.dumps(message)

        destination = PREFIX_TOPIC_MESSAGE + method

        self.put(json_message, destination=destination)

    def call(self, method, params, callback_success, callback_error):
        """Call remote method."""
        trans_id = self.last_id + 1
        self.last_id = trans_id

        self.callbacks_success[trans_id] = callback_success
        self.callbacks_error[trans_id] = callback_error

        message = {JSONRPC_KEY_JSONRP: JSONRPC_KEY_VERSION,
                    JSONRPC_KEY_METHOD: method,
                    JSONRPC_KEY_ID: trans_id,
                    JSONRPC_KEY_PARAMS: params}

        json_message = json.dumps(message)

        destination = PREFIX_TOPIC_JOB + method

        self.put(json_message, destination=destination,
                 conf={JSONRPC_KEY_REPLY_TO: self.reply_to_topic})

    def _process_message(self, message):
        """Process a message and call callback, method  o notification."""

        # FIXME: MSG_QUIT is temporary, must be removed
        if message.body == MSG_QUIT:
            self.listen = False
            return
        else:
            rpco = JSONRPCObject(message)
            # TODO: Check if the method is register or not
            # and return an error if it is not registered
            if rpco.is_job:
                try:
                    if len(rpco.params)>0:
                        params = json.loads(rpco.params)
                    else:
                        params = {}
                    result = self.methods[rpco.method](**params)
                    # if it is a job I must send a response
                    response = {JSONRPC_KEY_JSONRP: JSONRPC_KEY_VERSION,
                                JSONRPC_KEY_ID: rpco._id,
                                JSONRPC_KEY_RESULT: result}
                    json_response = json.dumps(response)
                    self.put(json_response, destination=rpco.reply_to)
                except Exception, e:
                    # TODO: Must implement specific errors. This is a
                    # general error
                    error = {JSONRPC_KEY_ERROR_CODE: JSONRPC_VALUE_ERROR_INT_ERR,
                            JSONRPC_KEY_ERROR_MESSAGE: JSONRPC_VALUE_MSG_INT_ERR}

                    response = {JSONRPC_KEY_JSONRP: JSONRPC_KEY_VERSION,
                                JSONRPC_KEY_ID: rpco._id,
                                JSONRPC_KEY_ERROR: error}
                    json_response = json.dumps(response)
                    self.put(json_response, destination=rpco.reply_to)

            elif rpco.is_notification:
                self.notifiers[rpco.notification](**rpco.params)
            elif rpco.is_callback:
                if rpco.message_type == MSG_TYPE_CALLBACK_SUCCESS:
                    self.callbacks_success[rpco._id](rpco.result)
                else:
                    self.callbacks_error[rpco._id](rpco.error)

            else:
                raise NotImplementedError

    def run(self, threaded=False):
        """Listen for events on registered methods and notifications."""


        self.listen = True
        while self.listen == True:

            try:
                message = self.get()
            except:
                # TODO: log exception
                time.sleep(DEFAULT_RETRY_TIME)
            else:
                # TODO: log only in debug mode to a file
                self._process_message(message)
