# Python - MagnumBI Dispatch Client Library

MagnumBI Dispatch manages microservice communication and interaction simply.   
It is easy to develop and integrate with your small to medium sized development teams.   
To see more about MagnumBI Dispatch and download the server [click here](https://github.com/OptimalBI/magnumbi-dispatch-server)   

This is the client library for python. Too use see below.  
For the repository used to develop this libary [see here](https://github.com/OptimalBI/magnumbi-dispatch-python-development)

## Requirements
Python 3.3 or greater. Python 2.x not supported.

## Getting started
Clone this project into a subfolder of your project. See the example below for more details.

PyPI coming soon!

## Examples

```python
import magnumbi_depot

client = magnumbi_depot.DispatchClient(
    uri='127.0.0.1',
    port=6883,
    access_key='test',
    secret_key='token',
    ssl_verify=False
)

status_result = client.check_status() # Will return True if connection to the server functioned correctly.
```