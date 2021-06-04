from flask import Flask, render_template, request, jsonify

app = Flask(__name__)


@app.route('/', methods=['GET', 'POST']) # To render Homepage
def home_page():
    return render_template('index.html')

@app.route('/math', methods=['POST'])  # This will be called from UI
def math_operation():
    if (request.method=='POST'):
        operation=request.form['operation']
        num1=int(request.form['num1'])
        num2 = int(request.form['num2'])
        if(operation=='add'):
            r=num1+num2
            result= 'the sum of '+str(num1)+' and '+str(num2) +' is '+str(r)
        if (operation == 'subtract'):
            r = num1 - num2
            result = 'the difference of ' + str(num1) + ' and ' + str(num2) + ' is ' + str(r)
        if (operation == 'multiply'):
            r = num1 * num2
            result = 'the product of ' + str(num1) + ' and ' + str(num2) + ' is ' + str(r)
        if (operation == 'divide'):
            r = num1 / num2
            result = 'the quotient when ' + str(num1) + ' is divided by ' + str(num2) + ' is ' + str(r)
        return render_template('results.html',result=result)

@app.route('/via_postman', methods=['POST']) # for calling the API from Postman/SOAPUI
def math_operation_via_postman():
    if (request.method=='POST'):
        operation=request.json['operation']
        num1=int(request.json['num1'])
        num2 = int(request.json['num2'])
        if(operation=='add'):
            r=num1+num2
            result= 'the sum of '+str(num1)+' and '+str(num2) +' is '+str(r)
        if (operation == 'subtract'):
            r = num1 - num2
            result = 'the difference of ' + str(num1) + ' and ' + str(num2) + ' is ' + str(r)
        if (operation == 'multiply'):
            r = num1 * num2
            result = 'the product of ' + str(num1) + ' and ' + str(num2) + ' is ' + str(r)
        if (operation == 'divide'):
            r = num1 / num2
            result = 'the quotient when ' + str(num1) + ' is divided by ' + str(num2) + ' is ' + str(r)
        return jsonify(result)

@app.route('/via_squared', methods=['POST']) # for calling the API from Postman/SOAPUI
def math_operation_squared():
    if (request.method=='POST'):
        operation = request.json['operation']
        num1 = int(request.json['num1'])
        if (operation == 'squared'):
            r = num1 * num1
            result = 'The square root of ' + str(num1) + ' is ' + str(r)
        return jsonify(result)

@app.route('/gen_office_email', methods=['POST']) # for calling the API from Postman/SOAPUI
def generate_emailid():
    if (request.method=='POST'):
        first_name = request.json['first_name']
        last_name = request.json['last_name']
        office_id = int(request.json['office_id'])

        email = first_name + '.' + last_name + str(office_id) + '@company.com'
        return jsonify(email)

# GET Request
# here data is passed in the url
@app.route('/add')
def addition():
        num1 = request.args.get('num1')
        num2 = request.args.get('num2')
        result = int(num1)+int(num2)
        return "Addition Result is : {}".format(result)
# run this url in web browser http://127.0.0.1:5000/add?num1=6&num2=4

if __name__ == '__main__':
    app.run()
