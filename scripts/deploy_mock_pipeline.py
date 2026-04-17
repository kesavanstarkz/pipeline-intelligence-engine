import boto3
import os

def deploy():
    print("=== AWS Pipeline Mock Deployment ===")
    
    # Read credentials from powershell environment
    AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
    AWS_REGION = 'us-east-1'
    
    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
         print("❌ ERROR: Missing AWS credentials.")
         return
         
    print("➜ Authenticating with Boto3...")
    cf_client = boto3.client(
        'cloudformation',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
    
    # Read the CloudFormation SAM template we created earlier
    template_path = 'infra/sample-api-lambda-s3.yml'
    try:
        with open(template_path, 'r') as f:
            template_body = f.read()
    except FileNotFoundError:
        print(f"❌ ERROR: Cannot find {template_path}")
        return
        
    stack_name = 'mock-api-lambda-s3-pipeline'
    print(f"➜ Deploying CloudFormation stack: {stack_name}...")
    print("  (This provisions API Gateway, AWS Lambda, and S3 bucket natively)")
    
    try:
        # We need CAPABILITY_AUTO_EXPAND because SAM template is used
        cf_client.create_stack(
            StackName=stack_name,
            TemplateBody=template_body,
            Capabilities=['CAPABILITY_IAM', 'CAPABILITY_AUTO_EXPAND'],
        )
        print("➜ Stack creation initiated. Waiting for AWS to provision resources...")
        print("  This may take ~2 minutes. Please wait...")
        
        # Wait for the stack to finish building
        waiter = cf_client.get_waiter('stack_create_complete')
        waiter.wait(StackName=stack_name)
        print("\n✅ MOCK PIPELINE DEPLOYMENT SUCCESSFUL!")
        print("Your agent should now be able to scan and discover the API Gateway -> Lambda -> S3 flow.")
        
    except cf_client.exceptions.AlreadyExistsException:
        print("➜ Stack already exists. Attempting an update...")
        try:
            cf_client.update_stack(
                StackName=stack_name,
                TemplateBody=template_body,
                Capabilities=['CAPABILITY_IAM', 'CAPABILITY_AUTO_EXPAND'],
            )
            print("➜ Updating stack... Please wait...")
            waiter = cf_client.get_waiter('stack_update_complete')
            waiter.wait(StackName=stack_name)
            print("\n✅ MOCK PIPELINE UPDATE SUCCESSFUL!")
        except Exception as e:
            if "No updates are to be performed" in str(e):
                print("\n✅ Pipeline is already active and up to date in AWS!")
            else:
                print(f"\n❌ Error updating stack: {e}")
    except Exception as e:
        print(f"\n❌ Error creating stack: {e}")

if __name__ == '__main__':
    deploy()
