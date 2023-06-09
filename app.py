from flask import Flask
import datetime

app = Flask(__name__)

@app.route("/")
def hello_world():
    current_year = datetime.datetime.now().year
    return f"The year is {current_year}"


if __name__ == '__main__':
    app.run()