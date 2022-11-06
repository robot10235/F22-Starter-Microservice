from flask import Flask, Response, request
from datetime import datetime
import json
from columbia_student_resource import ColumbiaStudentResource
from flask_cors import CORS

# Create the Flask application object.
app = Flask(__name__,
            static_url_path='/',
            static_folder='static/class-ui/',
            template_folder='web/templates')

CORS(app)


@app.get("/api/health")
def get_health():
    t = str(datetime.now())
    msg = {
        "name": "F22-Starter-Microservice",
        "health": "Very Good",
        "at time": t
    }

    # DFF TODO Explain status codes, content type, ... ...
    result = Response(json.dumps(msg), status=200, content_type="application/json")

    return result


@app.route("/api/students/<uni>", methods=["GET", "PUT", "DELETE"])
def get_student_by_uni(uni):
    if request.method == "GET":
        result = ColumbiaStudentResource.get_by_key(uni)
        if result:
            rsp = Response(json.dumps(result), status=200, content_type="application.json")
        else:
            rsp = Response("NOT FOUND", status=404, content_type="text/plain")
    elif request.method == "PUT":
        data = request.get_json()
        result = ColumbiaStudentResource.update_by_key(uni, data)
        if result > 0:
            rsp = Response("Update OK", status=200, content_type="application.json")
        else:
            rsp = Response("NOT FOUND", status=404, content_type="text/plain")
    else:
        result = ColumbiaStudentResource.delete_by_key(uni)
        if result > 0:
            rsp = Response("DELETE OK", status=200, content_type="application.json")
        else:
            rsp = Response("NOT FOUND", status=404, content_type="text/plain")
    return rsp


@app.route("/api/students", methods=["GET", "POST"])
def get_student():
    if request.method == "GET":
        result = ColumbiaStudentResource.get_all()
        if result:
            rsp = Response(json.dumps(result), status=200, content_type="application.json")
        else:
            rsp = Response("NOT FOUND", status=404, content_type="text/plain")
    elif request.method == "POST":
        data = request.get_json()
        result = ColumbiaStudentResource.add_one(data)
        if result > 0:
            rsp = Response("INSERT OK", status=200, content_type="application.json")
        else:
            rsp = Response("NOT FOUND", status=404, content_type="text/plain")
    return rsp


@app.route("/api/students/<page_num>", methods=["GET"])
def get_student(page_num):
    result = ColumbiaStudentResource.get_page(page_num)
    if result:
        rsp = Response(json.dumps(result), status=200, content_type="application.json")
    else:
        rsp = Response("NOT FOUND", status=404, content_type="text/plain")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5011)

