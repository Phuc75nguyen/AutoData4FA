from PIL import Image  # pip install pillow
img = Image.open("logo.png")
img.save("app.ico", sizes=[(256,256), (128,128), (64,64), (32,32), (16,16)])
