from json import load, loads
from multiprocessing.dummy import Pool
from sys import stderr
from time import time

from loguru import logger
from requests import post, get
from web3 import Web3, HTTPProvider
from web3.auto import w3

logger.remove()
logger.add(stderr, format="<white>{time:HH:mm:ss}</white>"
                          " | <level>{level: <8}</level>"
                          " | <cyan>{line}</cyan>"
                          " - <white>{message}</white>")


def format_keys(value: str) -> str:
    if not value.startswith('0x'):
        return f'0x{value}'

    return value


class Main:
    @staticmethod
    def register_user(current_address: str) -> None:
        while True:
            r = None

            try:
                r = get(f'https://api.rabbithole.gg/v1/quests/{current_address}',
                        timeout=10)

                if loads(r.text).get('quests'):
                    return

            except Exception as error:
                if r and '<title>Application Error</title>' not in r.text:
                    logger.error(f'{current_address} | Wrong Response: {r.text}, error: {error}')

                else:
                    logger.error(f'{current_address} | Wrong Response, error: {error}')

    @staticmethod
    def swap_tokens(current_key: str,
                    current_address: str) -> bool:
        nonce = provider.eth.getTransactionCount(current_address)

        build_tx_data = {
            'chainId': provider.eth.chain_id,
            'gasPrice': provider.eth.gas_price,
            'from': current_address,
            'nonce': nonce,
            'value': w3.toWei(0.1, 'ether')
        }

        transaction = balancer_contract.functions.swap(
            ('0xc9357cc86fa836b1761caad9b13fce0863cf281a000100000000000000000add',
             0,
             '0x0000000000000000000000000000000000000000',
             '0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174',
             100000000000000000,
             '0x'),
            (
                current_address,
                False,
                current_address,
                False
            ),
            0,
            int(time()) + 500) \
            .buildTransaction(build_tx_data)

        estimate_gas = provider.eth.estimateGas(transaction)
        build_tx_data['gas'] = estimate_gas

        signed_txn = w3.eth.account.signTransaction(transaction,
                                                    private_key=current_key)

        provider.eth.sendRawTransaction(signed_txn.rawTransaction)
        tx_hash = w3.toHex(w3.keccak(signed_txn.rawTransaction))

        tx_receipt = provider.eth.wait_for_transaction_receipt(tx_hash)

        if tx_receipt['status'] == 1:
            logger.success(f'{current_address} | [1/3] | {tx_hash}')
            return True

        else:
            logger.error(f'{current_address} | {tx_hash}')
            return False

    @staticmethod
    def get_mint_receipts_data(current_address: str) -> tuple:
        while True:
            r = None

            try:
                r = post('https://api.rabbithole.gg/quest/mint-receipt',
                         json={
                             'address': current_address,
                             'questId': '76fc16f5-8044-4783-ba10-ecbc3da9d4bd'
                         },
                         timeout=10)

                return loads(r.text)['signature'], loads(r.text)['hash']

            except Exception as error:
                if r and '<title>Application Error</title>' not in r.text:
                    logger.error(f'{current_address} | Wrong Response: {r.text}, error: {error}')

                else:
                    logger.error(f'{current_address} | Wrong Response, error: {error}')

    @staticmethod
    def mint_receipt_contract(current_key: str,
                              current_address: str,
                              mint_hash: str,
                              mint_sign: str) -> bool:
        nonce = provider.eth.getTransactionCount(current_address)

        build_tx_data = {
            'chainId': provider.eth.chain_id,
            'gasPrice': provider.eth.gas_price,
            'from': current_address,
            'nonce': nonce
        }

        transaction = mint_receipt_contract.functions.mintReceipt('76fc16f5-8044-4783-ba10-ecbc3da9d4bd',
                                                                  mint_hash,
                                                                  mint_sign).buildTransaction(build_tx_data)

        estimate_gas = provider.eth.estimateGas(transaction)
        build_tx_data['gas'] = estimate_gas

        signed_txn = w3.eth.account.signTransaction(transaction,
                                                    private_key=current_key)

        provider.eth.sendRawTransaction(signed_txn.rawTransaction)
        tx_hash = w3.toHex(w3.keccak(signed_txn.rawTransaction))

        tx_receipt = provider.eth.wait_for_transaction_receipt(tx_hash)

        if tx_receipt['status'] == 1:
            logger.success(f'{current_address} | [2/3] | {tx_hash}')
            return True

        else:
            logger.error(f'{current_address} | {tx_hash}')
            return False

    @staticmethod
    def claim_bal(current_key: str,
                  current_address: str):
        nonce = provider.eth.getTransactionCount(current_address)

        build_tx_data = {
            'chainId': provider.eth.chain_id,
            'gasPrice': provider.eth.gas_price,
            'from': current_address,
            'nonce': nonce
        }

        transaction = claim_contract.functions.claim().buildTransaction(build_tx_data)

        estimate_gas = provider.eth.estimateGas(transaction)
        build_tx_data['gas'] = estimate_gas

        signed_txn = w3.eth.account.signTransaction(transaction,
                                                    private_key=current_key)

        provider.eth.sendRawTransaction(signed_txn.rawTransaction)
        tx_hash = w3.toHex(w3.keccak(signed_txn.rawTransaction))

        tx_receipt = provider.eth.wait_for_transaction_receipt(tx_hash)

        if tx_receipt['status'] == 1:
            logger.success(f'{current_address} | [3/3] | {tx_hash}')
            return True

        else:
            logger.error(f'{current_address} | {tx_hash}')
            return False

    def main_work(self,
                  current_key: str) -> None:
        while True:
            try:
                current_address = provider.toChecksumAddress(w3.eth.account.from_key(current_key).address)

            except Exception as error:
                logger.error(f'{current_key} | Unexpected Error: {error}')

            else:
                break

        while True:
            try:
                self.register_user(current_address=current_address)

            except Exception as error:
                logger.error(f'{current_key} | Unexpected Error: {error}')

            else:
                logger.success(f'{current_address} | Адрес успешно зарегистрирован')
                break

        while True:
            try:
                if self.swap_tokens(current_key=current_key,
                                    current_address=current_address):
                    break

            except Exception as error:
                logger.error(f'{current_address} | Unexpected Error: {error}')

        while True:
            try:
                mint_sign, mint_hash = self.get_mint_receipts_data(current_address=current_address)

            except Exception as error:
                logger.error(f'{current_key} | Unexpected Error: {error}')

            else:
                break

        while True:
            try:
                if self.mint_receipt_contract(current_key=current_key,
                                              current_address=current_address,
                                              mint_hash=mint_hash,
                                              mint_sign=mint_sign):
                    break

            except Exception as error:
                logger.error(f'{current_address} | Unexpected Error: {error}')

        while True:
            try:
                if self.claim_bal(current_key=current_key,
                                  current_address=current_address):
                    break

            except Exception as error:
                logger.error(f'{current_address} | Unexpected Error: {error}')


def wrapper(current_key: str) -> None:
    try:
        MainObj.main_work(current_key=current_key)

    except Exception as error:
        logger.error(f'{current_key} | Unexpected Error: {error}')


if __name__ == '__main__':
    MainObj = Main()

    with open('settings.json', 'r', encoding='utf-8-sig') as file:
        settings_json = load(file)

    with open('claim_abi.json', 'r', encoding='utf-8-sig') as file:
        claim_abi = file.read().strip().replace('\n', '').replace(' ', '')

    with open('balancer_abi.json', 'r', encoding='utf-8-sig') as file:
        balancer_abi = file.read().strip().replace('\n', '').replace(' ', '')

    with open('mint_receipt_abi.json', 'r', encoding='utf-8-sig') as file:
        mint_receipt_abi = file.read().strip().replace('\n', '').replace(' ', '')

    with open('accounts.txt', 'r', encoding='utf-8-sig') as file:
        accounts_data = [format_keys(value=row.strip()) for row in file]

    POLYGON_RPC: str = settings_json['polygon_rpc']
    CLAIM_CONTRACT_ADDRESS: str = w3.toChecksumAddress(settings_json['claim_contract'])
    BALANCER_CONTRACT_ADDRESS: str = w3.toChecksumAddress(settings_json['balancer_contract'])
    MINT_RECEIPT_CONTRACT_ADDRESS: str = w3.toChecksumAddress(settings_json['mint_receipt_contract'])

    logger.success(f'Успешно загружено {len(accounts_data)} кошельков')

    provider = Web3(HTTPProvider(POLYGON_RPC))
    claim_contract = provider.eth.contract(address=CLAIM_CONTRACT_ADDRESS, abi=claim_abi)
    balancer_contract = provider.eth.contract(address=BALANCER_CONTRACT_ADDRESS, abi=balancer_abi)
    mint_receipt_contract = provider.eth.contract(address=MINT_RECEIPT_CONTRACT_ADDRESS, abi=mint_receipt_abi)

    threads = int(input('Threads: '))
    print('')

    with Pool(processes=threads) as executor:
        executor.map(wrapper, accounts_data)

    logger.success(f'Работа успешно завершена')
    input('\nPress Enter To Exit..')
