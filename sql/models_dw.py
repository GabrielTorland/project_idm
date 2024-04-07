from sqlalchemy import Column, String, DECIMAL, BigInteger, ForeignKey, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class DimUsers(Base):
    tablename = 'DimUsers'
    user_id = Column(String(9), primarykey=True)

class DimCategories(Base):
    tablename = 'DimCategories'
    category_id = Column(String(19), primary_key=True)
    category_code = Column(String(38), default=None)

class DimProducts(Base):
    __tablename = 'DimProducts'
    product_id = Column(String(9), primary_key=True)
    brand = Column(String(28), default=None)
    price = Column(DECIMAL(10, 2))

class FactSales(Base):
    __tablename = 'FactSales'
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
