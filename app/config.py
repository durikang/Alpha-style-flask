import os

class Config:
    UPLOAD_FOLDER = './uploads'
    MERGED_FOLDER = './merged'

class DevelopmentConfig(Config):
    DEBUG = True

class ProductionConfig(Config):
    DEBUG = False
