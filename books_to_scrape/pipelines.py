from itemadapter import ItemAdapter
from sqlalchemy import create_engine, Column, Integer, String, Float, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class Book(Base):
    __tablename__ = 'books'

    id = Column(Integer, primary_key=True)
    url = Column(String(255))
    title = Column(Text)
    upc = Column(String(255))
    product_type = Column(String(255))
    price_excl_tax = Column(Float)
    price_incl_tax = Column(Float)
    tax = Column(Float)
    price = Column(Float)
    availability = Column(Integer)
    num_reviews = Column(Integer)
    stars = Column(Integer)
    category = Column(String(255))
    description = Column(Text)

class BooksToScrapePipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        # Strip all whitespaces from strings
        field_names = adapter.field_names()
        for field_name in field_names:
            if field_name != 'description':
                value = adapter.get(field_name)
                adapter[field_name] = value.strip()

        # Category & Product Type --> switch to lowercase
        lowercase_keys = ['category', 'product_type']
        for lowercase_key in lowercase_keys:
            value = adapter.get(lowercase_key)
            adapter[lowercase_key] = value.lower()

        # Price --> convert to float
        price_keys = ['price', 'price_excl_tax', 'price_incl_tax', 'tax']
        for price_key in price_keys:
            value = adapter.get(price_key)
            value = value.replace('£', '')
            adapter[price_key] = float(value)

        # Availability --> extract the number of books in stock
        availability_string = adapter.get('availability')
        split_string_array = availability_string.split('(')
        if len(split_string_array) < 2:
            adapter['availability'] = 0
        else:
            availability_array = split_string_array[1].split(' ')
            adapter['availability'] = int(availability_array[0])

        # Reviews --> convert string to a number
        num_reviews_string = adapter.get('num_reviews')
        adapter['num_reviews'] = int(num_reviews_string)

        # Stars --> convert text to a number
        stars_string = adapter.get('stars')
        split_stars_array = stars_string.split(' ')
        stars_text_value = split_stars_array[1].lower()
        star_values = {
            'zero': 0,
            'one': 1,
            'two': 2,
            'three': 3,
            'four': 4,
            'five': 5
        }
        adapter['stars'] = star_values.get(stars_text_value, 0)

        return item

class SaveBooksToSQLite:
    def __init__(self):
        self.engine = create_engine('sqlite:///books.db')
        self.Session = sessionmaker(bind=self.engine)

    def open_spider(self, spider):
        Base.metadata.create_all(self.engine)

    def process_item(self, item, spider):
        session = self.Session()
        book = Book(
            url=item["url"],
            title=item["title"],
            upc=item["upc"],
            product_type=item["product_type"],
            price_excl_tax=item["price_excl_tax"],
            price_incl_tax=item["price_incl_tax"],
            tax=item["tax"],
            price=item["price"],
            availability=item["availability"],
            num_reviews=item["num_reviews"],
            stars=item["stars"],
            category=item["category"],
            description=str(item["description"])
        )
        session.add(book)
        session.commit()

        return item

    def close_spider(self, spider):
        self.Session.close()
