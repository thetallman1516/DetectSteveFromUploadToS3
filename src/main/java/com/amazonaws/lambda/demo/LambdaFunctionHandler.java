/**
 * This program runs text recognition on an image after it is uploaded to an S3 bucket.
 * It then publishes the result to an SNS topic.
 * The trigger for this lambda is a PUT event of a .png or .jpg
 */
package com.amazonaws.lambda.demo;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.List;

import javax.imageio.ImageIO;
import javax.imageio.stream.ImageInputStream;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.AmazonRekognitionException;
import com.amazonaws.services.rekognition.model.DetectTextRequest;
import com.amazonaws.services.rekognition.model.DetectTextResult;
import com.amazonaws.services.rekognition.model.Image;
import com.amazonaws.services.rekognition.model.TextDetection;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;

public class LambdaFunctionHandler implements RequestHandler<S3Event, String> {

    private AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();

    public LambdaFunctionHandler() {}

    // Test purpose only.
    LambdaFunctionHandler(AmazonS3 s3) {
        this.s3 = s3;
    }

    @Override
    public String handleRequest(S3Event event, Context context) {
        context.getLogger().log("Received event: " + event);
        try {
        	//Text Rekognition often gives an error if the lambda calls it without a 2 second delay. 
        	//It's likely that it's trying to process the image before it has been fully uploaded
			Thread.sleep(2000);
		} catch (InterruptedException e2) {			
			e2.printStackTrace();
		}

        // Get the object from the event and show its content type
        String bucket = event.getRecords().get(0).getS3().getBucket().getName();
        String key = event.getRecords().get(0).getS3().getObject().getKey();
        
        S3Object s3Image = s3.getObject(new GetObjectRequest(bucket, key));
        
        ImageInputStream iin;
        BufferedImage img;
        boolean steveFound = false;
        
		try {
			//creates a BufferedImage from the S3Object
			iin = ImageIO.createImageInputStream(s3Image.getObjectContent());
			img = ImageIO.read(iin);
			
			for(int i = 0; i<4; i++) {
				//generates an Image from the BufferedImage img  
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				ImageIO.write(img, "jpg", baos);
				ByteBuffer byteBuffer = ByteBuffer.wrap(baos.toByteArray());
				Image inputImage = new Image().withBytes(byteBuffer);
				
				steveFound = steveDetected(inputImage);
				
				if(steveFound==true)break;
				
				img = rotateClockwise90(img);
				
			}       
            
			publishResult(steveFound);			
			context.getLogger().log(String.valueOf(steveFound));
            return String.valueOf(steveFound);
        } catch (Exception e) {
        	 e.printStackTrace();
             context.getLogger().log(String.format(
                 "Error getting object %s from bucket %s. Make sure they exist and"
                 + " your bucket is in the same region as this function.", key, bucket));                       
        }
        return key;
    }
    
    /**
     * This method runs text recognition on an image and returns if "steve" is found 
     * @param input an image to be searched
     * @return whether "steve" has been detected
     * @throws Exception
     */
    public static boolean steveDetected(Image input) throws Exception {
    	

        AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder.defaultClient();

        
        DetectTextRequest request = new DetectTextRequest()
                .withImage(input);
      
        boolean steveFound = false;

        try {
        	
           DetectTextResult result = rekognitionClient.detectText(request);
           List<TextDetection> textDetections = result.getTextDetections();
           
           
           for (TextDetection text: textDetections) {
        	   	   String detectedText =  text.getDetectedText();
        	   	   if(detectedText.equalsIgnoreCase("steve")) {
        	   		   steveFound = true;
        	   	   }
                
           }
        } catch(AmazonRekognitionException e) {
           e.printStackTrace();
        }
        return steveFound;
     }
    
    /**This method takes a BufferedImage and rotates it 90 degrees clockwise
    Code source https://coderanch.com/t/485958/java/Rotating-buffered-image
    @param src source image
    @return dest the source image rotated 90 degrees clockwise    
    */
    public static BufferedImage rotateClockwise90(BufferedImage src) {
        int w = src.getWidth();
        int h = src.getHeight();
        BufferedImage dest = new BufferedImage(h, w, src.getType());
        for (int y = 0; y < h; y++) 
            for (int x = 0; x < w; x++) 
                dest.setRGB(y, w - x - 1, src.getRGB(x, y));
        return dest;
    }
	
    /**
     * Publishes the parameter steveFound to a SNS topic
     * @param steveFound the result of searching text for "steve"
     */
    public static void publishResult(boolean steveFound) {
    	
    	String msg = "Was Steve detected : ";
    	String topicArn = "arn:aws:sns:us-east-1:049648601596:SteveDetectedTopic";
    	AmazonSNS sns = AmazonSNSClient.builder().build();    	
    	sns.publish(topicArn, msg + String.valueOf(steveFound));
    }	
}