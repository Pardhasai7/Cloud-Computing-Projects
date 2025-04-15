from flask import Flask, request
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

app = Flask(__name__)

df = pd.read_csv('/home/ubuntu/classification_face_images_1000.csv')
classification_dict = dict(zip(df['Image'], df['Results']))

executor = ThreadPoolExecutor(max_workers=4)

@app.route('/', methods=['POST'])
def classify_image():
    if 'inputFile' not in request.files:
        return 'No file part', 400
    file = request.files['inputFile']
    if file.filename == '':
        return 'No selected file', 400
    if file:
        future = executor.submit(process_image, file)
        return future.result()

def process_image(file):
    filename = file.filename.split('.')[0]
    result = classification_dict.get(filename, 'Not found')
    return f"{filename}:{result}"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, threaded=True)
