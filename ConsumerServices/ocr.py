import pytesseract
from PIL import Image, ImageEnhance
import cv2
import numpy as np
import easyocr
import matplotlib.pyplot as plt
import keras_ocr

# Initialize the detector and recognizer
pipeline = keras_ocr.pipeline.Pipeline()

# Initialize the OCR reader
reader = easyocr.Reader(['en', 'kn'])  # English and Kannada languages

# Open an image file
folder='Data/CustomerData/Aadhar/'
files=['Aadhar.png','adhmummy.JPG']
for file in files:
    # Load the image
    image = keras_ocr.tools.read('image.png')
    # Perform text detection
    boxes = pipeline.recognize([image])[0]
    # Display the detected text regions
    fig, axs = plt.subplots(figsize=(10, 10))
    keras_ocr.tools.drawAnnotations(image=image, predictions=boxes, ax=axs)
    plt.imshow(image)
    plt.axis('off')
    plt.show()

    # Extract text from each detected region
    for box in boxes:
        img_crop = keras_ocr.tools.warpBox(image, box)
        prediction = pipeline.recognize([img_crop])[0]
        print(' '.join(prediction[0][0]))


    print("hi")



"""

 # Easy OCR
    image = cv2.imread(folder+file)
    # Convert the image to grayscale
    gray_image = cv2.cvtColor(np.asarray(image), cv2.COLOR_BGR2GRAY)
    #apply thresholds
    thresh_image = cv2.threshold(gray_image, 0, 255, cv2.THRESH_BINARY | cv2.THRESH_OTSU)[1]
    # Read the image and perform OCR
    result = reader.readtext(thresh_image)
# Path to the Tesseract executable   
pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'
# Open an image file
folder='Data/CustomerData/Aadhar/'
files=['Aadhar.png','adhmummy.JPG']
for file in files:
    image = Image.open(folder+file)
    #preprocess image
    # Convert the image to grayscale
    gray_image = cv2.cvtColor(np.asarray(image), cv2.COLOR_BGR2GRAY)
    #apply thresholds
    thresh_image = cv2.threshold(gray_image, 0, 255, cv2.THRESH_BINARY | cv2.THRESH_OTSU)[1]
    # Noise reduction using Gaussian blur
    blurred_image = cv2.GaussianBlur(thresh_image, (3, 3), 0)
    # Perform OCR on the image
    languages = 'kan+eng'
    text = pytesseract.image_to_string(thresh_image,lang=languages)
    # Print the extracted text
    print("hi")"""