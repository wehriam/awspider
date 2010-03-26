import specs
from twisted.internet.protocol import ClientCreator
from twisted.internet import reactor
from txamqp.client import TwistedDelegate
from txamqp.protocol import AMQClient
import txamqp.spec

def createClient(amqp_host, amqp_vhost, amqp_port=5672):
    amqp_spec = txamqp.spec.loadString(specs.v0_8)
    amqp_delegate = TwistedDelegate()
    client = ClientCreator(reactor, 
        AMQClient, 
        delegate=amqp_delegate,
        vhost=amqp_vhost,
        spec=amqp_spec).connectTCP(amqp_host, amqp_port)
    return client