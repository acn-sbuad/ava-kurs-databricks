import re

def convert_price_to_clean(price: str) -> str:
    """
    Removes special characters from a price value represented in USD.
    Arguments: 
        price: Price as a string with special characters
    Returns: 
        Price as a string without special characters such as $ and ,
    """
    return re.sub(r"[\$,]", "", price)