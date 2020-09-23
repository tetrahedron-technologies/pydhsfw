import sys
import os
import cv2
import time
import numpy as np
import math

import base64
import io
import json

import requests

from google.cloud import automl_v1beta1
from google.cloud.automl_v1beta1.proto import service_pb2

# need these for GCP predictions if using the Googel cloud hardware
project_id = "340753686888"
model_id = "IOD4547828582109413376"

# for predictions using the Google Cloud Platform 
# 'content' is base-64-encoded image data.
def get_prediction(content, project_id, model_id):
   prediction_client = automl_v1beta1.PredictionServiceClient()

   name = 'projects/{}/locations/us-central1/models/{}'.format(project_id, model_id)
   payload = {'image': {'image_bytes': content }}
   params = {}
   request = prediction_client.predict(name, payload, params)
   #print(request)
   return request  # waits till request is returned

# for prediction using our docker container
def container_predict(image_file_path, image_key, port_number=8501):
   """Sends a prediction request to TFServing docker container REST API.

   Args:
      image_file_path: Path to a local image for the prediction request.
      image_key: Your chosen string key to identify the given image.
      port_number: The port number on your device to accept REST API calls.
   Returns:
      The response of the prediction request.
    """

   with io.open(image_file_path, 'rb') as image_file:
      encoded_image = base64.b64encode(image_file.read()).decode('utf-8')

   # The example here only shows prediction with one image. You can extend it
   # to predict with a batch of images indicated by different keys, which can
   # make sure that the responses corresponding to the given image.
   instances = {
      'instances': [
         {'image_bytes': {'b64': str(encoded_image1)},
          'key': image_key}
      ]
   }

   # This example shows sending requests in the same server that you start
   # docker containers. If you would like to send requests to other servers,
   # please change localhost to IP of other servers.
   url = 'http://localhost:{}/v1/models/default:predict'.format(port_number)
   #url = 'http://localhost:{}/v1/models/0001:predict'.format(port_number)
   #url = 'http://localhost:{}/v1/models/0002:predict'.format(port_number)

   response = requests.post(url, data=json.dumps(instances))
   print(response.json())

def cv_size(img):
   return tuple(img.shape[1::-1])

def draw_bb(fn,ul,lr):

   image = cv2.imread(fn)
   s = cv_size(image)
   w = s[0]
   h = s[1]
   print(w,h)

   # Window name in which image is displayed 
   window_name = 'Image'

   # represents the top left corner of rectangle 
   start_point = (math.floor(ul[0] * w), math.floor(ul[1] * h)) 

   # represents the bottom right corner of rectangle 
   end_point = (math.ceil(lr[0] * w), math.ceil(lr[1] * h))  

   # Red color in BGR 
   color = (0, 0, 255) 

   # Line thickness of 1 px 
   thickness = 1

   # Using cv2.rectangle() method 
   # Draw a rectangle with red line borders of thickness of 1 px 
   image = cv2.rectangle(image, start_point, end_point, color, thickness)

   outfn = "test_" + os.path.basename(fn)
   outdir = os.path.dirname(fn)
   outfile = os.path.join(outdir,"bboxes",outfn)
   print(outfile)

   cv2.imwrite(outfile,image)

def draw_nope(fn):
   image = cv2.imread(fn)
   s = cv_size(image)
   w = s[0]
   h = s[1]
   print(w,h)
   font = cv2.FONT_HERSHEY_SIMPLEX
   oimage = cv2.putText(image,'NOPE',(10,200), font, 4,(0,0,125),4,cv2.LINE_AA)
   outfn = "nope_" + os.path.basename(fn)
   outdir = os.path.dirname(fn)
   outfile = os.path.join(outdir,"bboxes",outfn)
   print(outfile)
   cv2.imwrite(outfile,image)


   outfn = "test_" + os.path.basename(fn)
   outdir = os.path.dirname(fn)
   outfile = os.path.join(outdir,outfn)
   print(outfile)

def do_the_things(path):
   start = time.time()
   for filename in os.listdir(path):
      f = os.path.join(path,filename)
      if os.path.isfile(f):
         print(filename)
      
         ul = None
         lr = None
         try:
            #x1,y1,x2,y2 = predict(f)
            #ul = (x1,y1)
            #lr = (x2,y2)
            #draw_bb(f,ul,lr)
            
            # direct call to teh docer predict function
            # need help parsing output
            container_predict(f,"123456",5000)
         except:
            print("nope")
            draw_nope(f)
   end = time.time()
   print("time to run: {}".format(end - start))

# used for the GCP Cloud prediction
def predict(fn):
   with open(fn, 'rb') as ff:
      content = ff.read()

   gcp_res = get_prediction(content, project_id, model_id)
   #print(gcp_res.display_name)

   for result in gcp_res.payload:
      print("loop is predicted to be: {}".format(result.display_name))
      print("                  score: {}".format(result.image_object_detection.score))
      bb_ul = result.image_object_detection.bounding_box.normalized_vertices[0]
      bb_ul_x = result.image_object_detection.bounding_box.normalized_vertices[0].x
      bb_ul_y = result.image_object_detection.bounding_box.normalized_vertices[0].y
      print("                    ULx: {:6.4f} ULy: {:6.4f}".format(bb_ul_x,bb_ul_y))
      bb_lr = result.image_object_detection.bounding_box.normalized_vertices[1]
      bb_lr_x = result.image_object_detection.bounding_box.normalized_vertices[1].x
      bb_lr_y = result.image_object_detection.bounding_box.normalized_vertices[1].y
      print("                    LRx: {:6.4f} LRy: {:6.4f}".format(bb_lr_x,bb_lr_y))

      return(bb_ul_x,bb_ul_y,bb_lr_x,bb_lr_y)

def display():
   pass
   # Displaying the image
   #cv2.imshow(window_name, image)
   #cv2.waitKey(0)
   #cv2.destroyAllWindows()

if __name__ == '__main__':
   path = sys.argv[1]
   #project_id = sys.argv[2]
   #model_id = sys.argv[3]

   do_the_things(path)