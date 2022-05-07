import os

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship, Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.inspection import inspect
from sqlalchemy.dialects import postgresql

Base = declarative_base()


association_table = Table('association_city_customer', Base.metadata,
    Column('city_id', ForeignKey('city.id'), primary_key=True),
    Column('customer_id', ForeignKey('customer.id'), primary_key=True)
)

class Country(Base):
    __tablename__ = 'country'

    id = Column(Integer, primary_key=True)
    country = Column(String)
    # Child
    cities = relationship("City", back_populates="country")


class City(Base):
    __tablename__ = 'city'

    id = Column(Integer, primary_key=True)
    city = Column(String)
    # Parent
    country_id = Column(Integer, ForeignKey("country.id"))
    country = relationship("Country", back_populates="cities")
    # Child
    customers = relationship("Customer", secondary=association_table)


class Customer(Base):
    __tablename__ = 'customer'

    id = Column(Integer, primary_key=True)
    customer = Column(String)


def main():
    # load excel file into dataframe
    try:
        df = pd.read_excel('data.xlsx')
    except Exception as e:
        print('Unable to access csv file', repr(e))

    # filter out canada and dublin
    df = df.drop(df.loc[df['Country'] == 'Canada'].index)
    df = df.drop(df.loc[df['City'] == 'Dublin'].index)

    country = df[["Country"]].drop_duplicates()
    city = df[["City", "Country"]]
    customerlist = df["Customer"].str.split('|')
    customer = df[["City"]]
    customer = pd.concat([customer, customerlist], axis=1)
    customer = customer.explode("Customer")

    #print(country)
    #print(city)
    #print(customer)

    try:
        engine = create_engine('postgresql://postgres:root@localhost:5432/postgres2')
    except Exception as e:
        print('Unable to access postgresql database', repr(e))

    # Deleting old tables
    Base.metadata.drop_all(engine, tables=[Base.metadata.tables["association_city_customer"], Base.metadata.tables["country"], Base.metadata.tables["city"], Base.metadata.tables["customer"]])

    # Creating neu tables
    Base.metadata.create_all(engine)

    # Populate tables
    session = Session(engine)
    try:
        for i, row in country.iterrows():
            print('Country:  ' + row['Country'])
            cou = Country(
                country=row['Country']
            )
            session.add(cou)
            for j, row2 in city.iterrows():
                if row['Country'] == row2['Country']:
                    print('  City:  ' + row2['City'])
                    cit = City(
                        city=row2['City'],
                        country=cou
                    )
                    session.add(cit)
                    for k, row3 in customer.iterrows():
                        if row2['City'] == row3['City']:
                            print('     Customer:  ' + row3['Customer'])
                            cus = Customer(
                                customer=row3['Customer'],
                            )
                            cit.customers.append(cus)

    except Exception as e:
        session.rollback()
        print('Unable to populate tables', repr(e))
    else:
        session.commit()


if __name__ == '__main__':
    main()
