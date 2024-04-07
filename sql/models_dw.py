from sqlalchemy import Column, String, DECIMAL, BigInteger, ForeignKey, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class User(Base):
    tablename = 'Users'
    userid = Column(String(9), primarykey=True)

class Category(Base):
    tablename = 'Categories'
    category_id = Column(String(19), primary_key=True)
    category_code = Column(String(38), default=None)

class Product(Base):
    __tablename = 'Products'
    product_id = Column(String(9), primary_key=True)
    brand = Column(String(28), default=None)
    price = Column(DECIMAL(10, 2))

class Fact(Base):
    __tablename = 'Events'
    fact_id = Column(BigInteger, primary_key=True, autoincrement=True)
    date = Column(Date)
    unit_price = Column(DECIMAL(10, 2))
    quantity = Column(BigInteger)
    product_id = Column(String(9), ForeignKey('Products.product_id'))
    category_id = Column(String(9), ForeignKey('Category.category_id'))
    user_id = Column(String(9), ForeignKey('Users.user_id'))
    product = relationship("Product")
    category = relationship("Category")
    user = relationship("User")