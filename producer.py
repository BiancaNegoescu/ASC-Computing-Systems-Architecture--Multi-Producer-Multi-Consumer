"""
This module represents the Producer.

Computer Systems Architecture Course
Assignment 1
March 2021
"""

from threading import Thread
import time


class Producer(Thread):
    """
    Class that represents a producer.
    """

    def __init__(self, products, marketplace, republish_wait_time, **kwargs):
        """
        Constructor.

        @type products: List()
        @param products: a list of products that the producer will produce

        @type marketplace: Marketplace
        @param marketplace: a reference to the marketplace

        @type republish_wait_time: Time
        @param republish_wait_time: the number of seconds that a producer must
        wait until the marketplace becomes available

        @type kwargs:
        @param kwargs: other arguments that are passed to the Thread's __init__()
        """
        Thread.__init__(self, **kwargs)
        self.products = products
        self.marketplace = marketplace
        self.republish_wait_time = republish_wait_time
        self.producer_id = self.marketplace.register_producer()

    def run(self):
        while 1:
            i = 0
            while i < len(self.products):
                quantity = self.products[i][1]
                product = self.products[i][0]
                j = 0
                while j < quantity:
                    publish_status = self.marketplace.publish(self.producer_id, product)
                    if publish_status:
                        time.sleep(self.products[i][2]) # asteapta
                        j += 1 # trece la produsul urmator
                    else:
                        time.sleep(self.republish_wait_time)
                i += 1
