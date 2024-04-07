from sqlalchemy import Column, String, DECIMAL, TIMESTAMP, BigInteger, Integer, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class DimUser(Base):
    __tablename__ = 'dim_user'
    user_id = Column(String(9), primary_key=True)
    sales = relationship("FactSales", back_populates="user")

class DimCategory(Base):
    __tablename__ = 'dim_category'
    category_id = Column(String(19), primary_key=True)
    category_code = Column(String(38))
    products = relationship("DimProduct", back_populates="category")

class DimProduct(Base):
    __tablename__ = 'dim_product'
    product_id = Column(String(9), primary_key=True)
    category_id = Column(String(19), ForeignKey('dim_category.category_id'))
    brand = Column(String(28))
    unit_price = Column(DECIMAL(10, 2))
    category = relationship("DimCategory", back_populates="products")
    sales = relationship("FactSales", back_populates="product")

class FactSales(Base):
    __tablename__ = 'fact_sales'
    sale_id = Column(BigInteger, primary_key=True, autoincrement=True)
    sale_time = Column(TIMESTAMP)
    product_id = Column(String(9), ForeignKey('dim_product.product_id'))
    user_id = Column(String(9), ForeignKey('dim_user.user_id'))
    quantity = Column(Integer)
    unit_price = Column(DECIMAL(10, 2))
    product = relationship("DimProduct", back_populates="sales")
    user = relationship("DimUser", back_populates="sales")