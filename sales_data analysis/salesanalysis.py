import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv('amazon.csv')

def clean_currency(value):
    if isinstance(value, str):
        return float(value.replace('₹', '').replace(',', '').strip())
    return value

def clean_discount_percentage(value):
    if isinstance(value, str):
        return float(value.replace('%', '').strip())
    return value

def clean_rating(value):
    try:
        return float(value)
    except ValueError:
        return None

def clean_rating_count(value):
    if isinstance(value, str):
        cleaned = ''.join(char for char in value if char.isdigit())
        return int(cleaned) if cleaned else None
    return value

df['discounted_price'] = df['discounted_price'].apply(clean_currency)
df['actual_price'] = df['actual_price'].apply(clean_currency)
df['discount_percentage'] = df['discount_percentage'].apply(clean_discount_percentage)
df['rating'] = df['rating'].apply(clean_rating)
df['rating_count'] = df['rating_count'].apply(clean_rating_count)

try:
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='*********',
        database='data_analysis'
    )
    cursor = connection.cursor()

    sql = """
        INSERT INTO sales_data (
            product_id, product_name, category, discounted_price, actual_price,
            discount_percentage, rating, rating_count, about_product, user_id,
            user_name, review_id, review_title, review_content, img_link, product_link
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """

    for index, row in df.iterrows():
        values = (
            row.get('product_id', None), row.get('product_name', None), row.get('category', None),
            row.get('discounted_price', None), row.get('actual_price', None),
            row.get('discount_percentage', None), row.get('rating', None),
            row.get('rating_count', None), row.get('about_product', None),
            row.get('user_id', None), row.get('user_name', None), row.get('review_id', None),
            row.get('review_title', None), row.get('review_content', None),
            row.get('img_link', None), row.get('product_link', None)
        )

        cursor.execute(sql, values)

    connection.commit()
except mysql.connector.Error as err:
    print(f"Error: {err}")
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()

top_10 = df.nlargest(10, 'discounted_price')

plt.figure(figsize=(10, 6))
plt.barh(top_10['product_name'], top_10['discounted_price'], color='skyblue')
plt.xlabel('Discounted Price (₹)')
plt.ylabel('Product Name')
plt.title('Top 10 Products with Highest Discounted Prices')
plt.grid(True)
plt.tight_layout()
plt.show()
