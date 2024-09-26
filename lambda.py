import boto3

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Define bucket name and folders
    bucket_name = 'patterniq-ui-datasets'
    user_input_folder = 'New_datasets/'
    cloud_datasets_folder = 'Cloud_Datasets/'

    # Get metadata of all images from both folders
    user_input_images = get_images_metadata_from_s3(bucket_name, user_input_folder)
    cloud_datasets_images = get_images_metadata_from_s3(bucket_name, cloud_datasets_folder)

    # Compare images based on file size and metadata
    comparison_results = compare_images(user_input_images, cloud_datasets_images)

    # Log the results in CloudWatch
    print("Comparison Results:")
    for result in comparison_results:
        print(result)

    return {
        'statusCode': 200,
        'body': comparison_results
    }

def get_images_metadata_from_s3(bucket_name, folder):
    images_metadata = []
    s3_objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder)

    if 'Contents' in s3_objects:
        for obj in s3_objects['Contents']:
            key = obj['Key']
            if key.endswith(('.png', '.jpg', '.jpeg')):  # Only process image files
                # Get the size of the image file from the metadata
                metadata = s3.head_object(Bucket=bucket_name, Key=key)
                file_size = metadata['ContentLength']
                images_metadata.append((key, file_size))
    return images_metadata

def compare_images(user_images, cloud_images):
    results = []

    for user_key, user_size in user_images:
        match_found = False

        for cloud_key, cloud_size in cloud_images:
            # Compare images based on file size
            if user_size == cloud_size:
                match_found = True
                results.append(f"Match found for {user_key} with {cloud_key}")
                break
        
        if not match_found:
            results.append(f"No match found for {user_key}")

    return results
