from flask import Flask
# from app.helper import EvilTransform

app = Flask(__name__)
app.config.from_object('config')

from app import views
