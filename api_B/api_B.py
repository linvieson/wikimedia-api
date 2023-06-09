from flask import Flask, jsonify, request


class CassandraClient:
    def __init__(self, host, port, keyspace):
        self.host = host
        self.port = port
        self.keyspace = keyspace
        self.session = None

    def connect(self):
        from cassandra.cluster import Cluster
        from cassandra.query import dict_factory
        cluster = Cluster([self.host], port=self.port)
        self.session = cluster.connect(self.keyspace)
        self.session.row_factory = dict_factory

    def close(self):
        self.session.shutdown()

    def execute(self, query):
        return self.session.execute(query)

    def select_domains(self):
        query = 'SELECT DISTINCT domain FROM wiki.pages_by_domain'
        return list(self.execute(query))

    def select_pages(self, user_id):
        query = 'SELECT * FROM wiki.pages_by_user WHERE user_id = %s'
        return list(self.session.execute(self.session.prepare(query), user_id))

    def select_pages_for_domain(self, domain):
        query = 'SELECT COUNT(*) FROM wiki.pages_by_domain WHERE domain = %s'
        return list(self.session.execute(self.session.prepare(query), domain))

    def select_for_page_id(self, page_id):
        query = 'SELECT * FROM wiki.pages_by_domain WHERE uid = %s'
        return list(self.session.execute(self.session.prepare(query), page_id))

    def select_for_timestamp(self, start, end):
        query = 'SELECT * FROM wiki.pages_by_timestamp WHERE TODATE(rev_timestamp) >= %s AND TODATE(rev_timestamp) <= %s GROUP BY user_id'
        return list(self.session.execute(self.session.prepare(query), (start, end)))


app = Flask(__name__)
client = CassandraClient(host='cassandra-node', port=9042, keyspace='wiki')
client.connect()


# Endpoint 1: Return the list of existing domains for which pages were created
@app.route('/statistics/domains', methods=['GET'])
def get_domains():
    result = client.select_domains()
    domains = [row.domain for row in result]
    return jsonify(domains)


# Endpoint 2: Return all the pages created by the user with a specified user_id
@app.route('/statistics/user/<int:user_id>', methods=['GET'])
def get_user_pages(user_id):
    result = client.select_pages(user_id)
    pages = [row.page_id for row in result]
    return jsonify(pages)


# Endpoint 3: Return the number of articles created for a specified domain
@app.route('/statistics/domain/<string:domain>', methods=['GET'])
def get_articles_count(domain):
    result = client.select_pages_for_domain(domain)
    return jsonify(result)


# Endpoint 4: Return the page with the specified page_id
@app.route('/statistics/page/<int:page_id>', methods=['GET'])
def get_page(page_id):
    result = client.select_for_page_id(page_id)
    page = result[0]
    return jsonify({
        'page_id': page.page_id,
        'title': page.title,
        'content': page.content,
        'user_id': page.user_id,
        'domain': page.domain
    })


# Endpoint 5: Return the id, name, and the number of created pages of all users who created at least one page in a specified time range
@app.route('/statistics/info', methods=['GET'])
def get_users_info():
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    result = client.select_for_timestamp(start_date, end_date)
    users_info = [{
        'user_id': row.user_id,
        'name': row.name,
        'page_count': row.page_count
    } for row in result]
    return jsonify(users_info)


if __name__ == '__main__':
    app.run()
