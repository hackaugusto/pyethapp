# -*- coding: utf8 -*-
"""Provides a simple way of testing JSON RPC commands."""
import json
from itertools import count

from tinyrpc.protocols.jsonrpc import JSONRPCProtocol
from tinyrpc.transports.http import HttpPostClientTransport

from ethereum import abi
from ethereum import slogging
from ethereum.keys import privtoaddr
from ethereum.transactions import Transaction
from ethereum.utils import denoms, int_to_big_endian, big_endian_to_int, normalize_address
from pyethapp.jsonrpc import address_encoder as _address_encoder
from pyethapp.jsonrpc import data_encoder, data_decoder, address_decoder
from pyethapp.jsonrpc import default_gasprice, default_startgas
from pyethapp.jsonrpc import quantity_encoder, quantity_decoder

log = slogging.get_logger('pyethapp.rpc_client')  # pylint: disable=invalid-name
z_address = '\x00' * 20  # pylint: disable=invalid-name
NONE = object()  # used to signal the default value of a mutable type


def address20(address):
    if address == '':
        return address

    if len(address) == '42':
        address = address[2:]

    if len(address) == 40:
        address = address.decode('hex')

    assert len(address) == 20

    return address


def address_encoder(address):
    return _address_encoder(normalize_address(address))


def block_tag_encoder(val):
    if isinstance(val, int):
        return quantity_encoder(val)
    elif val and isinstance(val, bytes):
        assert val in ('latest', 'pending')
        return data_encoder(val)
    else:
        assert not val


def topic_encoder(topic):
    assert isinstance(topic, (int, long))
    return data_encoder(int_to_big_endian(topic))


def topic_decoder(topic):
    return big_endian_to_int(data_decoder(topic))


class JSONRPCClient(object):
    protocol = JSONRPCProtocol()

    def __init__(self, host='127.0.0.1', port=4000, privkey=None, sender=None):
        """
        Args:
            port (int): The remote end port.
            sender (address): The sender address, used in `send_transaction`.
            privkey: The key needs used to do local signing.
            host (ipaddress): The remote end ip address.
        """
        http_url = 'http://{host}:{port}'.format(host=host, port=port)
        self.transport = HttpPostClientTransport(http_url)
        self.privkey = privkey
        self._sender = sender
        self.port = port

    def __repr__(self):
        return '<JSONRPCClient @%d>' % self.port

    @property
    def sender(self):
        if self.privkey:
            return privtoaddr(self.privkey)
        if self._sender is None:
            self._sender = self.coinbase
        return self._sender

    def call(self, method, *args):
        """ Do a JSON-RPC v2 call for the given `method`. """
        request = self.protocol.create_request(method, args)
        reply = self.transport.send_message(request.serialize())

        log.debug(json.dumps(json.loads(request.serialize()), indent=2))
        log.debug(reply)

        return self.protocol.parse_reply(reply).result

    __call__ = call

    def find_block(self, condition):
        """Query all blocks one by one and return the first one for which
        `condition(block)` evaluates to `True`.

        Args:
            condition (function): Predicate for the accepected block, it's
                return value will be converted to boolean.

        Returns:
            The first block that satifies `condiction(block)` if there isn't a
            good block.
        """
        for curr_block in count():
            block = self.call('eth_getBlockByNumber', quantity_encoder(curr_block), True)

            if condition(block) or not block:
                return block

    def new_filter(self, from_block="", to_block="", address=None, topics=NONE):
        """ Creates a filter object to notify when the state changes (logs). To
        check if the state has changed, call eth_getFilterChanges.

        Topics are order-dependent. A transaction with a log with topics [A, B]
        will be matched by the following topic filters:

        - `[]` Anything.
        - `[A]` A in first position (and anything after).
        - `[null, B]` Anything in first position AND B in second position (and
          anything after).
        - `[A, B]` A in first position AND B in second position (and anything
          after.
        - `[[A, B], [A, B]]` (A OR B) in first position AND (A OR B) in second
          position (and anything after).

        Args:
            from_block: Integer block number, or "latest" for the last mined
                block or "pending", "earliest" for not yet mined transactions.

            to_block: Integer block number, or "latest" for the last mined block
                or "pending", "earliest" for not yet mined transactions.

            address (address): Contract address or a list of addresses from
                which logs should originate.

            topics: List topics ids. Topics are order-dependent. Each topic
                can also be a nested list meaning logical "or".

        Returns:
            The new topic id.
        """
        data = dict()

        if topics is NONE:
            topics = []

        if from_block is not None:
            data['fromBlock'] = block_tag_encoder(from_block)

        if to_block is not None:
            data['toBlock'] = block_tag_encoder(to_block)

        if address is not None:
            data['address'] = address_encoder(address)

        if topics is not None:
            data['topics'] = [
                topic_encoder(topic)
                for topic in topics
            ]

        filter_id = self.call('eth_newFilter', data)

        return quantity_decoder(filter_id)

    def filter_changes(self, filter_id):
        """ Polling method for a filter, which returns an array of logs which
        occurred since last poll.

        Args:
            filter_id: The filter id.
        """
        filter_changes = self.call('eth_getFilterChanges', quantity_encoder(filter_id))

        if not filter_changes:
            return None

        if isinstance(filter_changes, bytes):
            return data_decoder(filter_changes)

        attribute_decoder = {
            'blockHash': data_decoder,
            'transactionHash': data_decoder,
            'data': data_decoder,
            'address': address_decoder,
            'topics': lambda x: [topic_decoder(t) for t in x],
            'blockNumber': quantity_decoder,
            'logIndex': quantity_decoder,
            'transactionIndex': quantity_decoder,
        }

        return [
            {
                attribute: attribute_decoder[attribute](value)
                for attribute, value in change_log.items()
                if value is not None
            }
            for change_log in filter_changes
        ]

    def eth_sendTransaction(self, nonce=None, sender='', to='', value=0, data='',  # pylint: disable=invalid-name,too-many-arguments
                            gasprice=default_gasprice, gas=default_startgas,
                            v=None, r=None, s=None):
        """ Creates new message call transaction or a contract creation, if the
        data field contains code.

        Args:
            nonce (int): This allows to overwrite your own pending transactions
                that use the same nonce.
            sender (address): The address the transaction is send from.
            to (address): (optional when creating new contract) The address the
                transaction is directed to.
            value (int): value send with this transaction.
            data: The compiled code of a contract OR the hash of the invoked
                method signature and encoded parameters. For details see Ethereum
                Contract ABI.
            gasprice (int): gasprice used for each paid gas.
            gas (int): gas provided for the transaction execution. It will
                return unused gas.

        Returns:
            The transaction hash, or the zero hash if the transaction is not
            yet available.

            Use eth_getTransactionReceipt to get the contract address, after
            the transaction was mined, when you created a contract.
        """
        assert sender or (v and r and s)

        call_data = dict()

        if nonce is not None:
            call_data['nonce'] = quantity_encoder(nonce)

        if sender is not None:
            call_data['from'] = address_encoder(sender)

        if to is not None:
            to = normalize_address(to, allow_blank=True)
            call_data['to'] = data_encoder(to)

        if value is not None:
            call_data['value'] = quantity_encoder(value)

        if data is not None:
            call_data['data'] = data_encoder(data)

        if gasprice is not None:
            call_data['gasPrice'] = quantity_encoder(gasprice)

        if gas is not None:
            call_data['gas'] = quantity_encoder(gas)

        if v is not None:
            call_data['v'] = quantity_encoder(v)

        if r is not None:
            call_data['r'] = quantity_encoder(r)

        if s is not None:
            call_data['s'] = quantity_encoder(s)

        res = self.call('eth_sendTransaction', call_data)
        return data_decoder(res)

    def eth_call(self, sender='', to='', value=0, data='',  # pylint: disable=invalid-name,too-many-arguments
                 startgas=default_startgas, gasprice=default_gasprice):
        """
        Executes a new message call immediately without creating a transaction
        on the block chain.

        Args:
            sender: The address the transaction is send from.
            to: The address the transaction is directed to.
            value (int): Value send with this transaction.
            data: Hash of the method signature and encoded parameters. For
                details see Ethereum Contract ABI.
            startgas (int): Amount of gas provided for the transaction execution.
                eth_call consumes zero gas, but this parameter may be needed by
                some executions.
            gasprice (int): gasPrice used for each paid gas.

        Returns:
            The return value of executed contract.
        """

        call_data = dict()

        if sender is not None:
            call_data['from'] = address_encoder(sender)

        if to is not None:
            call_data['to'] = data_encoder(to)

        if value is not None:
            call_data['value'] = quantity_encoder(value)

        if data is not None:
            call_data['data'] = data_encoder(data)

        if startgas is not None:
            call_data['gas'] = quantity_encoder(startgas)

        if gasprice is not None:
            call_data['gasPrice'] = quantity_encoder(gasprice)

        result = self.call('eth_call', call_data)

        return data_decoder(result)

    def blocknumber(self):
        return quantity_decoder(self.call('eth_blockNumber'))

    def nonce(self, address):
        if len(address) == 40:
            address = address.decode('hex')
        return quantity_decoder(
            self.call('eth_getTransactionCount', address_encoder(address), 'pending'))

    @property
    def coinbase(self):
        return address_decoder(self.call('eth_coinbase'))

    def balance(self, account):
        balance = quantity_decoder(
            self.call('eth_getBalance', address_encoder(account), 'pending')
        )
        return balance

    def gaslimit(self):
        return quantity_decoder(self.call('eth_gasLimit'))

    def lastgasprice(self):
        return quantity_decoder(self.call('eth_lastGasPrice'))

    def send_transaction(self, sender, to, value=0, data='', startgas=0,  # pylint: disable=invalid-name,too-many-arguments
                         gasprice=10 * denoms.szabo, nonce=None):
        """ Send a signed transaction if privkey is given. """
        assert self.privkey or sender

        if self.privkey:
            _sender = sender
            sender = privtoaddr(self.privkey)
            assert sender == _sender

        if nonce is None:
            nonce = self.nonce(sender)

        if not startgas:
            startgas = quantity_decoder(self.call('eth_gasLimit')) - 1

        transaction = Transaction(nonce, gasprice, startgas, to=to, value=value, data=data)

        if self.privkey:
            transaction.sign(self.privkey)

        tx_dict = transaction.to_dict()
        tx_dict.pop('hash')

        rename = [('gasprice', 'gasPrice'), ('startgas', 'gas')]
        for python_name, jsonrpc_name in rename:
            tx_dict[jsonrpc_name] = tx_dict.pop(python_name)

        tx_dict['sender'] = sender

        res = self.eth_sendTransaction(**tx_dict)
        assert len(res) in (20, 32)

        return res.encode('hex')

    def new_abi_contract(self, _abi, address):
        sender = self.sender or privtoaddr(self.privkey)
        return ABIContract(sender, _abi, address, self.eth_call, self.send_transaction)


class AbiMethod(object):
    valid_kargs = set(('gasprice', 'startgas', 'value'))

    def __init__(self, method_name, translator, sender, address, call_func, transact_func):  # pylint: disable=too-many-arguments
        self.method_name = method_name
        self._translator = translator
        self.sender = normalize_address(sender)
        self.address = normalize_address(address)
        self.call_func = call_func
        self.transact_func = transact_func

    def transact(self, *args, **kargs):
        assert set(kargs.keys()).issubset(self.valid_kargs)
        data = self._translator.encode(self.method_name, args)

        txhash = self.transact_func(
            sender=self.sender,
            to=self.address,
            value=kargs.pop('value', 0),
            data=data,
            **kargs
        )

        return txhash

    def call(self, *args, **kargs):
        assert set(kargs.keys()).issubset(self.valid_kargs)
        data = self._translator.encode(self.method_name, args)

        res = self.call_func(
            sender=self.sender,
            to=self.address,
            value=kargs.pop('value', 0),
            data=data,
            **kargs
        )

        if res:
            res = self._translator.decode(self.method_name, res)
            res = res[0] if len(res) == 1 else res

        return res

    def __call__(self, *args, **kargs):
        if self._translator.function_data[self.method_name]['is_constant']:
            return self.call(*args, **kargs)
        else:
            return self.transact(*args, **kargs)


class ABIContract(object):  # pylint: disable=too-few-public-methods
    """ Proxy for a contract.

    The ABIContract instance will have it's methods setup accordingly to the
    `_abi` given in the constructor, making all `_abi` methods avaiable
    throught this proxy.
    """

    def __init__(self, sender, _abi, address, call_func, transaction_func):
        """
        Args:
            sender (address): The sender address.
            _abi (string): The contract's interface in JSON.
            address (address): The target address.
            call_function (function): The callback for a method call.
            transaction_function (function): The callback for a transaction call.
        """
        self.abi = _abi
        self.address = address = normalize_address(address)

        translator = abi.ContractTranslator(_abi)

        for method_name in translator.function_data:
            method = AbiMethod(
                method_name,
                translator,
                sender,
                address,
                call_func,
                transaction_func,
            )

            signature = translator.function_data[method_name]['signature']

            type_name = [
                '{type} {name}'.format(type=type_, name=name)
                for (type_, name) in signature
            ]

            method.__doc__ = '{method}({arguments})'.format(
                method=method_name,
                arguments=', '.join(type_name)
            )

            setattr(self, method_name, method)
