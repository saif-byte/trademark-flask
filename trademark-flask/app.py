from flask import Flask, request, render_template, url_for, jsonify, redirect, flash
from script import preprocess_dataframe ,create_class_dataframe,count_top_products_except_eu_countries,count_top_products_eu_countries,count_top_products_by_country, count_top_products_except_country
from CONSTANTS import PASSWORD
import pandas as pd
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

app = Flask(__name__)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/post_data", methods=["POST"])
def post_data():
    f = request.files['file']
    class_name = request.form["class"]
    country = request.form["country"]
    print(f , class_name , country)
    try:
        df = pd.read_excel(f)
    except:
        df = pd.read_csv(f)
    result = process_data(df, class_name, country)
    print(result)
    return render_template("results.html" , result  = result)

@app.route("/send_email", methods=["POST"])
def send_email():
    # Get the email body from the form submission
    email_body = request.form.get("email_body")

    # Replace these placeholders with your actual email configuration
    smtp_server = "smtp.gmail.com"
    smtp_port = 587
    sender_email = "saif4rmkhan@gmail.com"
    sender_password = PASSWORD
    recipient_email = "saif4rmkhan@gmail.com"

    try:
        # Create a connection to the SMTP server
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()

        # Log in to your email account
        server.login(sender_email, sender_password)

        # Create the email message
        msg = MIMEMultipart()
        msg["From"] = sender_email
        msg["To"] = recipient_email
        msg["Subject"] = "sending from Trademark App"  # Replace with your email subject
        body = email_body  # The email body from the form
        msg.attach(MIMEText(body, "plain"))

        # Send the email
        server.sendmail(sender_email, recipient_email, msg.as_string())
        print("Email sent successfully!")

        # Close the server connection
        server.quit()
        return render_template("email_sent.html")
    except Exception as e:
        print(f"Failed to send email: {e}")
        return render_template("email_sent.html")

def process_data(df, class_name, country):
    print(df)
    df = preprocess_dataframe(df)
    df_class = create_class_dataframe(df, class_name)
    eu_countries = [
    'Austria', 'Belgium', 'Bulgaria', 'Croatia', 'Republic of Cyprus', 'Czech Republic',
    'Denmark', 'Estonia', 'Finland', 'France', 'Germany', 'Greece', 'Hungary', 'Ireland',
    'Italy', 'Latvia', 'Lithuania', 'Luxembourg', 'Malta', 'Netherlands', 'Poland',
    'Portugal', 'Romania', 'Slovakia', 'Slovenia', 'Spain', 'Sweden'
    ]
    if "EU Countries" in country:
        in_eu = count_top_products_eu_countries(df_class, eu_countries,top_values=10) 
        out_eu = count_top_products_except_eu_countries(df_class, eu_countries ,top_values=10)
        result = (in_eu,out_eu)

    else: 
        in_ct = count_top_products_by_country(df_class, country, top_values=10) 
        out_ct = count_top_products_except_country(df_class, country, top_values=10)
        result  = (in_ct , out_ct)

    return result

if __name__ == "__main__":
    # with app.app_context():
    #     db.create_all()  # <--- create db object.
    app.run(debug=True)
