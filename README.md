# s3list

This Respository uses AWS CodePipeline to build out the following:
<ul>
<li>An <b>AWS Lambda</b>, that will provide a provide a lists of objects for files stored in an S3 bucket.</li>
<li>The <b>AWS IAM Role & Policy</b> for that Lambda's execution.</li>
<li>An API in the <b>Amazon API Gateways</b>, to provide access to the Lambda.</li>
<li>An <b>AWS WAF</b> to protect that API.</li>
</ul>
as well as any other necessary AWS resources to support the above services.