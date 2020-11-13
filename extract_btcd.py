from pytesseract import pytesseract

import cv2

image = cv2.imread('/home/simon/btcd.png')
y=451
x=188
h=32
w=97
crop = image[y:y+h, x:x+w]
pytesseract.tesseract_cmd = "tesseract"

print(pytesseract.image_to_string(crop))