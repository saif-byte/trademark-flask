#!/usr/bin/env python
# coding: utf-8

import pandas as pd
def preprocess_dataframe(df):

    
    # Convert all columns to lowercase
    df.columns = df.columns.str.lower()
    
    # Remove trailing and leading spaces from column names
    df.columns = df.columns.str.strip()
    
    # Remove trailing and leading spaces from values in each cell
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df["product descriptions (english, unofficial)"] = df["product descriptions (english, unofficial)"].str.replace(r"\[\d+\]", "")
    df["product descriptions (english, unofficial)"] = df["product descriptions (english, unofficial)"].str.strip().str.lower()

    df["classes"] = df["classes"].astype(str)

        
    return df


def create_class_dataframe(df , class_num):
    class_dataframe = df[df['classes'].apply(lambda x: str(class_num).lstrip('0') in x.split(', '))]
    return class_dataframe



def count_top_products_by_country(df, country_name, top_values):
    # Make a copy of the DataFrame to avoid modifying the original data
    df_copy = df.copy()
    
    # Convert the "country" column to lowercase for case-insensitive matching
    df_copy["country"] = df_copy["country"].str.lower()
    
    # Extract the product list after the first ":"
    df_copy["product descriptions (english, unofficial)"] = df_copy["product descriptions (english, unofficial)"].str.split(":", n=1).str[1]
    
    # Filter the DataFrame for the specified country
    country_df = df_copy.loc[df_copy["country"].str.contains(country_name, case=False, na=False)]
    
    # Split the "Product Description" column into separate products
    country_df["Product List"] = country_df["product descriptions (english, unofficial)"].str.split(";")
    
    # Explode the list of products to create separate rows for each product
    country_df_exploded = country_df.explode("Product List")
    
    # Group by "Country" and count the occurrences of each product
    product_counts = country_df_exploded.groupby("Product List").size().reset_index(name="Count")
    
    # Sort by count in descending order
    sorted_product_counts = product_counts.sort_values(by="Count", ascending=False)
    
    # Get the top products for the specified country
    top_products = sorted_product_counts.head(top_values)
    
    return top_products



def count_top_products_except_country(df, country_name, top_values):
    # Make a copy of the DataFrame to avoid modifying the original data
    df_copy = df.copy()
    
    # Convert the "country" column to lowercase for case-insensitive matching
    df_copy["country"] = df_copy["country"].str.lower()
    
    # Extract the product list after the first ":"
    df_copy.loc[:, "product descriptions (english, unofficial)"] = df_copy["product descriptions (english, unofficial)"].str.split(":", n=1).str[1]
    
    # Filter the DataFrame to exclude the specified country
    other_countries_df = df_copy.loc[~df_copy["country"].str.contains(country_name, case=False, na=False)]
    
    # Split the "Product Description" column into separate products
    other_countries_df["Product List"] = other_countries_df["product descriptions (english, unofficial)"].str.split(";")
    
    # Explode the list of products to create separate rows for each product
    other_countries_df_exploded = other_countries_df.explode("Product List")
    
    # Group by "Country" and count the occurrences of each product
    product_counts = other_countries_df_exploded.groupby("Product List").size().reset_index(name="Count")
    
    # Sort by count in descending order
    sorted_product_counts = product_counts.sort_values(by="Count", ascending=False)
    in_eu = count_top_products_by_country(df, country_name, top_values)
    merged_df = pd.merge(
        in_eu["Product List"],
        sorted_product_counts["Product List"],
        on="Product List",
        how="inner"
    )
    #sorted_product_counts = in_eu[~in_eu['Product List'].isin(sorted_product_counts['Product List'])]
    # Get the top products for the other countries
    df2_filtered = sorted_product_counts[~sorted_product_counts["Product List"].isin(merged_df["Product List"])]

    top_products = df2_filtered.head(top_values)
    
    return top_products

def count_top_products_except_eu_countries(df, eu_country_list, top_values):
    # Make a copy of the DataFrame to avoid modifying the original data
    df_copy = df.copy()

    # Convert the "country" column to lowercase for case-insensitive matching
    df_copy["country"] = df_copy["country"].str.lower()

    # Function to find approximate EU country matches
    def find_approximate_eu_country_match(country_name):
        for eu_country in eu_country_list:
            if eu_country.__contains__(country_name):
                return eu_country
        return None

    # Find the approximate matches for EU countries in the dataset
    df_copy["approx_eu_country"] = df_copy["country"].apply(find_approximate_eu_country_match)

    # Filter the DataFrame to exclude EU countries and their approximate matches
    other_countries_df = df_copy[df_copy["approx_eu_country"].isna()]

    # Extract the product list after the first ":"
    other_countries_df["product descriptions (english, unofficial)"] = other_countries_df["product descriptions (english, unofficial)"].str.split(":", n=1).str[1]

    # Split the "Product Description" column into separate products
    other_countries_df["Product List"] = other_countries_df["product descriptions (english, unofficial)"].str.split(";")

    # Explode the list of products to create separate rows for each product
    other_countries_df_exploded = other_countries_df.explode("Product List")

    # Group by "Country" and count the occurrences of each product
    product_counts = other_countries_df_exploded.groupby("Product List").size().reset_index(name="Count")

    # Sort by count in descending order
    sorted_product_counts = product_counts.sort_values(by="Count", ascending=False)
    in_eu = count_top_products_eu_countries(df, eu_country_list, top_values)
    merged_df = pd.merge(
        in_eu["Product List"],
        sorted_product_counts["Product List"],
        on="Product List",
        how="inner"
    )
    #sorted_product_counts = in_eu[~in_eu['Product List'].isin(sorted_product_counts['Product List'])]
    # Get the top products for the other countries
    df2_filtered = sorted_product_counts[~sorted_product_counts["Product List"].isin(merged_df["Product List"])]

    top_products = df2_filtered.head(top_values)
    
    return top_products


def count_top_products_eu_countries(df, eu_country_list, top_values):
    # Make a copy of the DataFrame to avoid modifying the original data
    df_copy = df.copy()

    # Convert the "country" column to lowercase for case-insensitive matching
    df_copy["country"] = df_copy["country"].str.lower()

    # Function to find approximate EU country matches
    def find_approximate_eu_country_match(country_name):
        for eu_country in eu_country_list:
            if eu_country.lower() in country_name:
                return eu_country
        return None

    # Find the approximate matches for EU countries in the dataset
    df_copy["approx_eu_country"] = df_copy["country"].apply(find_approximate_eu_country_match)

    # Filter the DataFrame to include only EU countries and their approximate matches
    eu_countries_df = df_copy[df_copy["approx_eu_country"].notna()]

    # Extract the product list after the first ":"
    eu_countries_df["product descriptions (english, unofficial)"] = eu_countries_df["product descriptions (english, unofficial)"].str.split(":", n=1).str[1]

    # Split the "Product Description" column into separate products
    eu_countries_df["Product List"] = eu_countries_df["product descriptions (english, unofficial)"].str.split(";")

    # Explode the list of products to create separate rows for each product
    eu_countries_df_exploded = eu_countries_df.explode("Product List")

    # Group by "Country" and count the occurrences of each product
    product_counts = eu_countries_df_exploded.groupby("Product List").size().reset_index(name="Count")

    # Sort by count in descending order
    sorted_product_counts = product_counts.sort_values(by="Count", ascending=False)

    # Get the top products for EU countries
    top_products = sorted_product_counts.head(top_values)

    return top_products






