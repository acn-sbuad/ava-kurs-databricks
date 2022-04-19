import pytest 
import pandas as pd

#locate the path of a csv file in the directory
file = 'data-files/airbnb.csv'
data = pd.read_csv(file)

proper_row_count = 2970
min_nights = 30
min_price = 0
max_price = 2100

columns_to_keep = [
  "host_is_superhost",
  "instant_bookable",
  "host_total_listings_count",
  "neighbourhood_cleansed",
  "property_type",
  "room_type",
  "accommodates",
  "bedrooms",
  "beds",
  "minimum_nights",
  "number_of_reviews",
  "review_scores_rating",
  "review_scores_accuracy",
  "review_scores_cleanliness",
  "review_scores_checkin",
  "review_scores_communication",
  "review_scores_location",
  "review_scores_value",
  "price",
  "longitude",
  "latitude", "belzebia"
]

#let's check if we have a right number of rows
def test_rows_count():
    row_count = len(data.index)
    assert row_count == proper_row_count

#test if the minimum_nights column includes values that we're not interested in
def test_min_nights():
    cur_min_nights = data[data['minimum_nights'] > min_nights]
    assert len(cur_min_nights) == 0

#let's test if the price range is satysfing  
def test_price_range():
    cur_price_range = data[(data['price'] < min_price) | (data['price'] > max_price)]
    assert len(cur_price_range) == 0

#let's check if the columns are in a proper order
def test_column_names():
    cur_column_names = list(data.columns)
    check = all(item in columns_to_keep for item in cur_column_names)
    assert check
