"""
This module represents the Marketplace.

Computer Systems Architecture Course
Assignment 1
March 2021
"""


from threading import Lock, currentThread


class Marketplace:
    """
    Class that represents the Marketplace. It's the central part of the implementation.
    The producers and consumers use its methods concurrently.
    """
    def __init__(self, queue_size_per_producer):
        """
        Constructor

        :type queue_size_per_producer: Int
        :param queue_size_per_producer: the maximum size of a queue associated with each producer
        """
        self.queue_size = queue_size_per_producer
        self.nr_of_producers = -1
        self.nr_of_carts = -1
        self.producers_lock = Lock()
        self.carts_lock = Lock()
        self.carts = {}
        self.products = [] # lista de produse
        self.nr_products_per_producer = [] # nr produse per producator
        self.products_of_producers = {} # (produs: producator)

    def register_producer(self):
        """
        Returns an id for the producer that calls this.
        """
        # idurile producatorilor incep de la 0
        with self.producers_lock:
            self.nr_of_producers += 1
        self.nr_products_per_producer.append(0)
        return self.nr_of_producers

    def publish(self, producer_id, product):
        """
        Adds the product provided by the producer to the marketplace

        :type producer_id: String
        :param producer_id: producer id

        :type product: Product
        :param product: the Product that will be published in the Marketplace

        :returns True or False. If the caller receives False, it should wait and then try again.
        """
        if self.nr_products_per_producer[producer_id] < self.queue_size:
            # adaug produsul la lista de produse
            self.products.append(product)
            # adaug produsul la producator
            self.products_of_producers[product] = producer_id
            # incrementez coada de produse ale producatorului
            self.nr_products_per_producer[producer_id] += 1
            return True
        return False

    def new_cart(self):
        """
        Creates a new cart for the consumer

        :returns an int representing the cart_id
        """
        with self.carts_lock:
            self.nr_of_carts += 1
        self.carts[self.nr_of_carts] = []
        return self.nr_of_carts

    def add_to_cart(self, cart_id, product):
        """
        Adds a product to the given cart. The method returns

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to add to cart

        :returns True or False. If the caller receives False, it should wait and then try again
        """
        with self.carts_lock:
            if product not in self.products:
                return False
            # se adauga produsul in cos
            self.carts[cart_id].append(product)
            self.products.remove(product)
            # producatorul produsului
            producer = self.products_of_producers[product]
            # nr produse ale producatorului scade
            self.nr_products_per_producer[producer] -=1
        return True

    def remove_from_cart(self, cart_id, product):
        """
        Removes a product from cart.

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to remove from cart
        """
        # se sterge produsul din cos
        self.carts[cart_id].remove(product)
        # adaug din nou produsul la producator
        with self.carts_lock:
            # producatorul produsului
            producer = self.products_of_producers[product]
            self.nr_products_per_producer[producer] +=1
        # adaug produsul la lista de produse disponibile
        self.products.append(product)

    def place_order(self, cart_id):
        """
        Return a list with all the products in the cart.

        :type cart_id: Int
        :param cart_id: id cart
        """
        products = self.carts[cart_id]
        for product in products:
            print(f'{currentThread().getName()} bought {product}')

        return products
