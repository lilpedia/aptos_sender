from multiprocessing.dummy import Pool
from os import getenv
from sys import stderr
from time import sleep
import random

from aptos_sdk.account import Account
from aptos_sdk.client import RestClient
from loguru import logger

logger.remove()
logger.add(stderr, format="<white>{time:HH:mm:ss}</white>"
                          " | <level>{level: <8}</level>"
                          " | <cyan>{line}</cyan>"
                          " - <white>{message}</white>")

NODE_URL = getenv("APTOS_NODE_URL", "https://fullnode.mainnet.aptoslabs.com/v1")
REST_CLIENT = RestClient(NODE_URL)


class App:
    def transfer_tokens(self,
                        private_key_source: str,
                        wallet_dest: str,
                        delay_min: int,
                        delay_max: int) -> None:
        try:
            current_account = Account.load_key(key=private_key_source)

        except ValueError:
            logger.error(f'{private_key_source} | Невалидный Private Key')
            return

        while True:
            try:
                account_balance = int(REST_CLIENT.account_balance(account_address=str(current_account.address())))
                gas_price = 130000

                if account_balance <= gas_price:
                    logger.info(f'{private_key_source} | Маленький баланс: {account_balance / 100000000}')
                    return

                tx_hash = REST_CLIENT.transfer(sender=current_account,
                                               recipient=wallet_dest,
                                               amount=account_balance - gas_price)

                logger.success(f'{private_key_source} | {tx_hash}')

                sleep(random.randint(delay_min, delay_max))

            except Exception as error:
                logger.error(f'{private_key_source} | {error}')

                if '{"message":"' in str(error):
                    return

            else:
                return


def transfer_wrapper(pair_of_keys: tuple):
    App().transfer_tokens(private_key_source=pair_of_keys[0], wallet_dest=pair_of_keys[1],
                          delay_min=pair_of_keys[2], delay_max=pair_of_keys[3])


if __name__ == '__main__':
    threads = int(input('Введите количество потоков: '))
    delay_min = int(input('Введите минимальную задержку в секундах: '))
    delay_max = int(input('Введите максимальную задержку в секундах: '))
    user_action = int(input('1. Перевод APT между кошельками\n'
                            'Введите ваше действие: '))

    if user_action == 1:
        with open('private_keys_sources.txt', 'r', encoding='utf-8-sig') as file:
            private_keys_sources = [row.strip() for row in file]

        with open('wallets_destinations.txt', 'r', encoding='utf-8-sig') as file:
            wallets_destinations = [row.strip() for row in file]

        logger.info(f'Успешно загружено {len(private_keys_sources)} private key\'s для кошельков источников')
        logger.info(f'Успешно загружено {len(wallets_destinations)} адресов для кошельков получателей')

        assert len(private_keys_sources) == len(wallets_destinations)

        with Pool(processes=threads) as executor:
            executor.map(transfer_wrapper, zip(private_keys_sources, wallets_destinations,
                                               [delay_min]*len(private_keys_sources),
                                               [delay_max]*len(private_keys_sources)))
