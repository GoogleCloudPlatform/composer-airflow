from datetime import datetime, timedelta
import dateutil.parser
import json
import logging

from flask import Flask, url_for, Markup, Blueprint, redirect, flash
from flask.ext.admin import Admin, BaseView, expose, AdminIndexView
from flask.ext.admin.contrib.sqla import ModelView
from flask import request
from wtforms import Form, DateTimeField

from pygments import highlight
from pygments.lexers import PythonLexer
from pygments.formatters import HtmlFormatter


import markdown
import chartkick

from core.settings import Session
from core import models
from core.models import State
from core import settings
from core import utils

dagbag = models.DagBag(settings.DAGS_FOLDER)
session = Session()

app = Flask(__name__)
app.secret_key = 'fluxified'

# Init for chartkick, the python wrapper for highcharts
ck = Blueprint(
    'ck_page', __name__,
    static_folder=chartkick.js(), static_url_path='/static')
app.register_blueprint(ck, url_prefix='/ck')
app.jinja_env.add_extension("chartkick.ext.charts")


# Date filter form needed for gantt and graph view
class DateTimeForm(Form):
    execution_date = DateTimeField("Execution date")


@app.route('/')
def index():
    return redirect(url_for('admin.index'))


class HomeView(AdminIndexView):
    """
    Basic home view, just showing the README.md file
    """
    @expose("/")
    def index(self):
        md = "".join(
            open(settings.BASE_FOLDER + '/README.md', 'r').readlines())
        content = Markup(markdown.markdown(md))
        return self.render('admin/index.html', content=content)
admin = Admin(app, name="Flux", index_view=HomeView(name='Home'))


class Flux(BaseView):

    def is_visible(self):
        return False

    @expose('/')
    def index(self):
        return self.render('admin/dags.html')

    @expose('/code')
    def code(self):
        dag_id = request.args.get('dag_id')
        dag = dagbag.dags[dag_id]
        code = "".join(open(dag.filepath, 'r').readlines())
        title = dag.filepath.replace(settings.BASE_FOLDER + '/dags/', '')
        html_code = highlight(
            code, PythonLexer(), HtmlFormatter(noclasses=True))
        return self.render(
            'admin/code.html', html_code=html_code, dag=dag, title=title)

    @expose('/log')
    def log(self):
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        execution_date = request.args.get('execution_date')
        dag = dagbag.dags[dag_id]
        loc = settings.BASE_LOG_FOLDER + "/{dag_id}/{task_id}/{execution_date}"
        loc = loc.format(**locals())
        try:
            f = open(loc)
            log = "".join(f.readlines())
        except:
            log = "Log file is missing"

        log = "<pre><code>" + log + "</code></pre>"
        title = "Logs for {task_id} on {execution_date}".format(**locals())
        html_code = log

        return self.render(
            'admin/code.html', html_code=html_code, dag=dag, title=title)

    @expose('/tree')
    def tree(self):
        dag_id = request.args.get('dag_id')
        dag = dagbag.dags[dag_id]
        action = request.args.get('action')

        base_date = request.args.get('base_date')
        if not base_date:
            base_date = datetime.now()
        else:
            base_date = dateutil.parser.parse(base_date)

        num_runs = request.args.get('num_runs')
        if not num_runs:
            num_runs = 45
        else:
            num_runs = int(num_runs)
        from_date = (base_date-(num_runs * dag.schedule_interval)).date()
        from_date = datetime.combine(from_date, datetime.min.time())

        if action == 'clear':
            task_id = request.args.get('task_id')
            task = dag.get_task(task_id)
            execution_date = request.args.get('execution_date')
            future = request.args.get('future') == "true"
            past = request.args.get('past') == "true"
            upstream = request.args.get('upstream') == "true"
            downstream = request.args.get('downstream') == "true"

            start_date = execution_date
            end_date = execution_date

            if future:
                end_date = None
            if past:
                start_date = None

            count = task.clear(
                start_date=start_date,
                end_date=end_date,
                upstream=upstream,
                downstream=downstream)

            flash("{0} task instances have been cleared".format(count))
            return redirect(url_for('flux.tree', dag_id=dag_id))

        dates = utils.date_range(
            from_date, base_date, dag.schedule_interval)
        task_instances = {}
        for ti in dag.get_task_instances(from_date):
            task_instances[(ti.task_id, ti.execution_date)] = ti

        def recurse_nodes(task):
            return {
                'name': task.task_id,
                'instances': [
                    utils.alchemy_to_dict(
                        task_instances.get((task.task_id, d))) or {
                            'execution_date': d.isoformat(),
                            'task_id': task.task_id
                        }
                    for d in dates],
                'children': [recurse_nodes(t) for t in task.upstream_list],
                'num_dep': len(task.upstream_list),
                'operator': task.task_type,
                'retries': task.retries,
                'owner': task.owner,
                'start_date': task.start_date,
                'end_date': task.end_date,
                'depends_on_past': task.depends_on_past,
            }
        if len(dag.roots) > 1:
            # d3 likes a single root
            data = {
                'name': 'root',
                'instances': [],
                'children': [recurse_nodes(t) for t in dag.roots]
            }
        else:
            data = recurse_nodes(dag.roots[0])

        data = json.dumps(data, indent=4, default=utils.json_ser)

        return self.render(
            'admin/tree.html',
            dag=dag, data=data)

    @expose('/graph')
    def graph(self):
        session = settings.Session()
        dag_id = request.args.get('dag_id')
        dag = dagbag.dags[dag_id]

        nodes = []
        edges = []
        for task in dag.tasks:
            nodes.append({
                'id': task.task_id,
                'value': {'label': task.task_id}
            })

        def get_downstream(task, edges):
            for t in task.upstream_list:
                edges.append({
                    'u': t.task_id,
                    'v': task.task_id,
                    'value': {'label': ''}
                })
                get_downstream(t, edges)

        for t in dag.roots:
            get_downstream(t, edges)

        dttm = request.args.get('execution_date')
        if dttm:
            dttm = dateutil.parser.parse(dttm)
        else:
            dttm = dag.latest_execution_date or datetime.now().date()

        form = DateTimeForm(data={'execution_date': dttm})

        task_instances = {
            ti.task_id: utils.alchemy_to_dict(ti)
            for ti in dag.get_task_instances(dttm, dttm)
        }
        tasks = {
            t.task_id: utils.alchemy_to_dict(t)
            for t in dag.tasks
        }
        session.commit()
        session.close()

        return self.render(
            'admin/graph.html',
            dag=dag,
            form=form,
            execution_date=dttm.isoformat(),
            task_instances=json.dumps(task_instances, indent=2),
            tasks=json.dumps(tasks, indent=2),
            nodes=json.dumps(nodes, indent=2),
            edges=json.dumps(edges, indent=2),)

    @expose('/duration')
    def duration(self):
        session = settings.Session()
        dag_id = request.args.get('dag_id')
        dag = dagbag.dags[dag_id]
        from_date = (datetime.today()-timedelta(30)).date()
        from_date = datetime.combine(from_date, datetime.min.time())

        all_data = []
        for task in dag.tasks:
            data = []
            for ti in task.get_task_instances(from_date):
                if ti.end_date:
                    data.append([
                        ti.execution_date.isoformat(),
                        int(ti.duration)
                    ])
            if data:
                all_data.append({'data': data, 'name': task.task_id})

        session.commit()
        session.close()

        return self.render(
            'admin/chart.html',
            dag=dag,
            data=all_data,
            height="500px",
        )

    @expose('/landing_times')
    def landing_times(self):
        session = settings.Session()
        dag_id = request.args.get('dag_id')
        dag = dagbag.dags[dag_id]
        from_date = (datetime.today()-timedelta(30)).date()
        from_date = datetime.combine(from_date, datetime.min.time())

        all_data = []
        for task in dag.tasks:
            data = []
            for ti in task.get_task_instances(from_date):
                if ti.end_date:
                    data.append([
                        ti.execution_date.isoformat(), (
                            ti.end_date - ti.execution_date
                        ).total_seconds()/(60*60)
                    ])
            all_data.append({'data': data, 'name': task.task_id})

        session.commit()
        session.close()

        return self.render(
            'admin/chart.html',
            dag=dag,
            data=all_data,
            height="500px",
        )

    @expose('/gantt')
    def gantt(self):

        session = settings.Session()
        dag_id = request.args.get('dag_id')
        dag = dagbag.dags[dag_id]

        dttm = request.args.get('execution_date')
        if dttm:
            dttm = dateutil.parser.parse(dttm)
        else:
            dttm = dag.latest_execution_date

        form = DateTimeForm(data={'execution_date': dttm})

        tis = dag.get_task_instances(dttm, dttm)
        tis = sorted(tis, key=lambda ti: ti.start_date)
        tasks = []
        data = []
        for i, ti in enumerate(tis):
            end_date = ti.end_date or datetime.now()
            tasks += [ti.task_id]
            color = State.color(ti.state)
            data.append({
                'x': i,
                'low': int(ti.start_date.strftime('%s')) * 1000,
                'high': int(end_date.strftime('%s')) * 1000,
                'color': color,
            })
        height = (len(tis) * 25) + 50
        session.commit()
        session.close()

        hc = {
            'chart': {
                'type': 'columnrange',
                'inverted': True,
                'height': height,
            },
            'xAxis': {'categories': tasks},
            'yAxis': {'type': 'datetime'},
            'title': {
                'text': None
            },
            'plotOptions': {
                'series': {
                    # 'borderColor': 'black',
                    'minPointLength': 5,
                },
            },
            'legend': {
                'enabled': False
            },
            'series': [{
                'data': data
            }]
        }
        return self.render(
            'admin/gantt.html',
            dag=dag,
            form=form,
            hc=json.dumps(hc, indent=4),
            height=height,
        )


admin.add_view(Flux(name='DAGs'))

# ------------------------------------------------
# Leveraging the admin for CRUD and browse on models
# ------------------------------------------------


class ModelViewOnly(ModelView):
    """
    Modifying the base ModelView class for non edit, browse only operations
    """
    named_filter_urls = True
    can_create = False
    can_edit = False
    can_delete = False
    column_display_pk = True


def filepath_formatter(view, context, model, name):
    url = url_for('flux.code', dag_id=model.dag_id)
    short_fp = model.filepath.replace(settings.BASE_FOLDER + '/dags/', '')
    link = Markup('<a href="{url}">{short_fp}</a>'.format(**locals()))
    return link


def dag_formatter(view, context, model, name):
    url = url_for('flux.tree', dag_id=model.dag_id, num_runs=45)
    link = Markup('<a href="{url}">{model.dag_id}</a>'.format(**locals()))
    return link


class DagModelView(ModelViewOnly):
    column_formatters = {
        'dag_id': dag_formatter,
        'filepath': filepath_formatter,
    }
    column_list = ('dag_id', 'task_count', 'filepath')
mv = DagModelView(models.DAG, session, name="DAGs")
admin.add_view(mv)


def dag_link(view, context, model, name):
    return model


class DagModelView(ModelViewOnly):
    column_formatters = {
        'dag_id': dag_link,
    }


class TaskModelView(ModelViewOnly):
    column_filters = ('dag_id', 'owner', 'start_date', 'end_date')
    column_formatters = {
        'dag': dag_formatter,
    }
mv = TaskModelView(
    models.BaseOperator, session, name="Tasks", category="Objects")
admin.add_view(mv)


class TaskInstanceModelView(ModelViewOnly):
    column_filters = ('dag_id', 'task_id', 'state', 'execution_date')
    column_list = (
        'state', 'dag_id', 'task_id', 'execution_date',
        'start_date', 'end_date', 'duration')
    can_delete = True
mv = TaskInstanceModelView(
    models.TaskInstance, session, name="Task Instances", category="Objects")
admin.add_view(mv)


class JobModelView(ModelViewOnly):
    column_default_sort = ('start_date', True)
mv = JobModelView(models.Job, session, name="Jobs", category="Objects")
admin.add_view(mv)


mv = ModelView(models.User, session, name="Users", category="Admin")
admin.add_view(mv)


class DatabaseConnectionModelView(ModelView):
    column_exclude_list = ('login', 'password',)
    form_choices = {
        'db_type': [
            ('mysql', 'MySQL',),
            ('oracle', 'Oracle',),
        ]
    }
mv = DatabaseConnectionModelView(
    models.DatabaseConnection, session,
    name="Database Connections", category="Admin")
admin.add_view(mv)


class LogModelView(ModelViewOnly):
    column_default_sort = ('dttm', True)
    column_filters = ('dag_id', 'task_id', 'execution_date')
mv = LogModelView(models.Log, session, name="Logs", category="Admin")
admin.add_view(mv)

if __name__ == "__main__":
    logging.info("Starting the web server.")
    app.run(debug=True)
