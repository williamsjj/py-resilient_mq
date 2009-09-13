####################################################################
# FILENAME: lib/resilient_mq.py
# PROJECT: py-resilient_mq
# DESCRIPTION: Wraps py-amqplib to handle failure/reconnection 
#              to AMQP servers.
# 
#   Requires:
#       * py-amqplib = 0.6.1
#       * demjson >= 1.4
#
########################################################################################
# (C)2009 DigiTar, All Rights Reserved
# Distributed under the BSD License
# 
# Redistribution and use in source and binary forms, with or without modification, 
#    are permitted provided that the following conditions are met:
#
#        * Redistributions of source code must retain the above copyright notice, 
#          this list of conditions and the following disclaimer.
#        * Redistributions in binary form must reproduce the above copyright notice, 
#          this list of conditions and the following disclaimer in the documentation 
#          and/or other materials provided with the distribution.
#        * Neither the name of DigiTar nor the names of its contributors may be
#          used to endorse or promote products derived from this software without 
#          specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY 
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES 
# OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT 
# SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, 
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED 
# TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR 
# BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN 
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN 
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH 
# DAMAGE.
#
########################################################################################

from amqplib import client_0_8 as amqp
import socket, struct
import demjson

class ResilientMQ(object):
    mq_conn = None
    mq_chan = None
    servers = []
    user = ""
    passwd = ""
    vhost = ""
    exchange = None
    queue = None
    routing_key = ""
    logging = True
    
    # Setup connection to MQ server
    def _connect(self):
        """Attempt to connect to spec'd MQ servers in order.

        Arguments:

            NONE

        Returns:

            SUCCESS - Nothing.

            FAILURE - No return, but raises Exception.
        """
        for server in self.servers:
            try:
                self.mq_conn = amqp.Connection(host=server, 
                                               userid=self.user, 
                                               password=self.passwd,
                                               virtual_host=self.vhost,
                                               connect_timeout=5)
                
                # Since errors are asynchronous, channel errors such as "Exchange does not exist." that occur
                # when calling basic_publish don't get noticed until a chan.wait() or chan.close(). So, on
                # process object instantiation we do a quick channel open/publish/channel close to make sure
                # there's basic sanity before we begin. This is an imperfect solution.
                # 
                # This needs to be replaced with txAMQP so errors can be handled asynchronously on every
                # basic_publish.
                self.mq_chan = self.mq_conn.channel()
                
                # Sanity check exchange (if spec'd)
                if self.exchange != None:
                    self.mq_chan.exchange_declare(exchange=self.exchange['name'], 
                                                  type=self.exchange['type'],
                                                  durable=self.exchange['durable'],
                                                  auto_delete=self.exchange['auto_delete'])
                if self.routing_key != "":
                    self.mq_chan.basic_publish(amqp.Message("Connection Test"),
                                               exchange=self.exchange['name'],
                                               routing_key="conn_test")
                                               
                # Declare queues and bindings (if spec'd) to prevent blackholing of messages post 
                # MQ cluster failover.
                if self.queue != None:
                    self.mq_chan.queue_declare(queue=self.queue['name'],
                                               durable=self.queue['durable'],
                                               auto_delete=self.queue['auto_delete'])
                                               
                    if self.routing_key != None:
                        self.mq_chan.queue_bind(queue=self.queue['name'],
                                                exchange=self.exchange['name'],
                                                routing_key=self.routing_key)
                self.mq_chan.close()
                self.mq_chan = self.mq_conn.channel()
                break
            except (socket.error,
                    struct.error,
                    amqp.AMQPConnectionException,
                    amqp.AMQPChannelException), e:
                self.mq_conn = None
                self.mq_chan = None
                
                if self.logging:
                    print "Problem establishing MQ connection to server %s: %s " \
                          % (str(server), str(repr(e)))
                          
        if self.mq_conn == None:
            raise Exception("Unable to create MQ connection to any supplied servers.")
        if self.mq_chan == None:
            raise Exception("Unable to establish an AMQP channel on the connected server (%s)." \
                            % server)
    
    def __init__(self, servers, vhost, user, passwd, exchange=None, 
                 queue=None, routing_key="", logging=True):
        """Initialize MQ connection (failing between servers if required) and 
        create exchange, queue and routing_key bindings if necessary.
        
        Arguments:
            
            servers (list) (required) - List of servers to try connecting to in "hostname:port" format.
            vhost (string) (required) - AMQP virtual host to bind to.
            user (string) (required) - Username to authenticate to MQ with.
            passwd (string) (required)- Password to authenticate to MQ with.
            
            exchange (dict) - If not None, exchange to create.
                    {"name" : 'exchange_name',
                     "type" : 'exchange_type',
                     "durable" : True/False,
                     "auto_delete : True/False" }
            queue (dict) - If not None, queue to create.
                    {"name" : 'queue_name',
                     "durable" : True/False,
                     "auto_delete" : True/False }
            routing_key (string) - If not "", routing key to use to bind queue to exchange.
            logging (boolean) - If True, print debugging/logging/status messages on errors.
            
        Returns:
        
            Nothing.
        """
        
        if not isinstance(servers, list):
            raise Exception("'server' argument must be a list of strings.")
        if len(servers) < 1:
            raise Exception("At least one server must be specified.")
        self.servers = servers
        
        self.vhost = vhost
        self.user = user
        self.passwd = passwd
        
        if isinstance(exchange, dict):
            for key in ['name', 'type', 'durable', 'auto_delete']:
                if not exchange.has_key(key):
                    raise Exception("Exchange dictionary missing exchange '%s' key." % key)
        self.exchange = exchange
        
        if isinstance(queue, dict):
            for key in ['name', 'durable', 'auto_delete']:
                if not queue.has_key(key):
                    raise Exception("Queue dictionary missing queue '%s' key." % key)
        self.queue = queue
        
        self.routing_key = routing_key
        self.logging = logging
        
        socket.setdefaulttimeout(10)
        self._connect()
        
       
    
    def publish(self, msg_body, exchange_name=None, routing_key=None,
                persistent=False, retry_attempts=2, **msg_properties):
        """Publishes a message to the spec'd exchange name using spec'd routing key. If no
        exchange or routing key is spec'd, we'll use the defaults (if any) spec'd at 
        instantiation.
        
        Arguments:
        
            msg_body (string) (required) - Message text to publish to the exchange.
            exchange_name (string) (required) - Name of the exchange to publish to. May be omitted
                    if an exchange was spec'd at instantiation.
            routing_key (string) - Routing key (if any) to tag on the message.
            persistent (boolean) - Defaults to False. Set to True if doing persistent messaging.
                    NB: For 'persistent' to mean anything, both the queue and the exchange
                        must be 'durable'.
            retry_attempts (int) - Defaults to 2. Number of times to try to publish the message
                    if a socket error is encountered or the connection/channel is lost.
            **msg_properties - Catch all for any additional parameters to be passed through
                    to the Message() class in py-amqplib.
        Returns:
        
            SUCCESS - Nothing.
            FAILURE - Nothing, but raises an exception if connection errors are encountered.
        """
        if exchange_name == None:
            if not isinstance(self.exchange, dict):
                raise Exception("No exchange specified now or at object initialization.")
            else:
                exchange_name = self.exchange['name']
        if routing_key == None:
            routing_key = self.routing_key
        
        msg = amqp.Message(body=msg_body, delivery_mode=int(persistent)+1, **msg_properties)
        
        # Make sure the connection/channel still exists.
        for retries in range(0,retry_attempts):
            try:
                if isinstance(self.mq_chan, amqp.Channel) and \
                   isinstance(self.mq_conn, amqp.Connection):
                    self.mq_chan.basic_publish(msg,
                                               exchange=exchange_name,
                                               routing_key=routing_key)
                    break
                else:
                    # Lost the connection somewhere...get it back.
                    self._connect()
            except socket.error, e:
                if retries < (retry_attempts - 1):
                    if self.logging:
                        print "Error sending message to MQ server. Reconnecting.\n%s" % str(repr(e))
                    
                    # Auto-reconnect...that's why we're resilient....
                    self._connect()
                else:
                    if self.logging:
                        print "Couldn't connect to MQ server (tried %s times). Discarding message." % \
                              str(retry_attempts)
                    raise socket.error(e) # Re-raise error since it's fatal now
    
    def publish_json(self, msg_body, **publish_properties):
        """Wrapper for publish() that encodes msg_body into a JSON string. Takes all the same args
        as publish() (passed through to publish()).

        Arguments:

            msg_body (any Python datatype) (required) - Object to JSON encode and publish to the exchange.
            
            ** Additional parameters must be passed as keyword arguments.
            ** See publish() method for additional parameter descriptions.
        
        Returns:

            SUCCESS - Nothing.
            FAILURE - Nothing, but raises an exception if connection errors are encountered.
        """
        
        self.publish(demjson.encode(msg_body), **publish_properties)