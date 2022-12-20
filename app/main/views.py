from __future__ import print_function

from flask import Flask, render_template, redirect, url_for, abort, flash, request,\
    current_app, make_response, g, jsonify
from flask import send_from_directory
from flask_login import login_required, current_user
from flask_sqlalchemy import get_debug_queries
from sqlalchemy import text
import os
import sys
import flask_sijax
import json
from werkzeug import secure_filename
import mimetypes
from os import path
import zipfile
import tarfile
import shutil
import tempfile
import pathlib

from . import main
from .forms import EditProfileForm, EditProfileAdminForm, PostForm, CommentForm
from .. import db
from ..models import Permission, Role, User, Post, Comment, Workflow, WorkItem, DataSource, Data, DataType, OperationSource, Operation
from ..decorators import admin_required, permission_required

from .ajax import WorkflowHandler
from ..util import Utility
#from ..io import PosixFileSystem, HadoopFileSystem, getFileSystem
from ..biowl.fileop import PosixFileSystem, HadoopFileSystem, GalaxyFileSystem, IOHelper
from ..biowl.dsl.parser import PhenoWLParser
from ..biowl.dsl.interpreter import Interpreter
from ..biowl.dsl.pygen import CodeGenerator
 
from ..biowl.timer import Timer
from ..models import Runnable, Status
from ..biowl.tasks import runnable_manager

from ..jobs import long_task, run_script, stop_script, sync_task_status_with_db, sync_task_status_with_db_for_user

app = Flask(__name__)
basedir = os.path.abspath(os.path.dirname(__file__))

@main.after_app_request
def after_request(response):
    for query in get_debug_queries():
        if query.duration >= current_app.config['PHENOPROC_SLOW_DB_QUERY_TIME']:
            current_app.logger.warning(
                'Slow query: %s\nParameters: %s\nDuration: %fs\nContext: %s\n'
                % (query.statement, query.parameters, query.duration,
                   query.context))
    return response


@main.route('/shutdown')
def server_shutdown():
    if not current_app.testing:
        abort(404)
    shutdown = request.environ.get('werkzeug.server.shutdown')
    if not shutdown:
        abort(500)
    shutdown()
    return 'Shutting down...'

def load_data_sources():
        # construct data source tree
    datasources = DataSource.query.all()
    datasource_tree = { 'type': DataType.Custom, 'children': [] }
    for ds in datasources:
        datasource = { 'datasource': ds.id, 'type': DataType.Root, 'base':'', 'path': ds.url, 'name': ds.name, 'children': []}
        if ds.id == 1:
            # hdfs tree
            try:
                hdfs = HadoopFileSystem()
                if current_user.is_authenticated:
                    datasource['children'].append(hdfs.make_json(ds.id, Utility.get_rootdir(ds.id), current_user.username))
                datasource['children'].append(hdfs.make_json(ds.id, Utility.get_rootdir(ds.id), current_app.config['PUBLIC_DIR']))
            except:
                pass

        elif ds.id == 2:
            # file system tree
            posixFS = PosixFileSystem()
            if current_user.is_authenticated:
                datasource['children'].append(posixFS.make_json(ds.id, Utility.get_rootdir(ds.id), current_user.username))
            datasource['children'].append(posixFS.make_json(ds.id, Utility.get_rootdir(ds.id), current_app.config['PUBLIC_DIR']))
 
        datasource_tree['children'].append(datasource)
        
    return datasource_tree

@main.route('/reloaddatasources', methods=['POST'])
def load_data_sources_json():
    return json.dumps(load_data_sources())

# @main.route('/', defaults={'id': ''}, methods = ['GET', 'POST'])
# @main.route('/workflow/<int:id>/', methods = ['GET', 'POST'])
# def index(id=None):
#               
#     id = Utility.ValueOrNone(id)
#     if id <= 0:
#         id = request.args.get('workflow')
#                                                                
#     if g.sijax.is_sijax_request:
#         # Sijax request detected - let Sijax handle it
#         g.sijax.register_object(WorkflowHandler)
#         return g.sijax.process_request()
# 
#     form = PostForm()
#     if current_user.can(Permission.WRITE_ARTICLES) and form.validate_on_submit():
#         post = Post(body=form.body.data, author=current_user._get_current_object())
#         db.session.add(post)
#         return redirect(url_for('.index'))
#     page = request.args.get('page', 1, type=int)
#     show_followed = False
#     if current_user.is_authenticated:
#         show_followed = bool(request.cookies.get('show_followed', ''))
#     if show_followed:
#         query = current_user.followed_posts
#     else:
#         query = Post.query
#     pagination = query.order_by(Post.timestamp.desc()).paginate(
#         page, per_page=current_app.config['PHENOPROC_POSTS_PER_PAGE'],
#         error_out=False)
#     posts = pagination.items
#     
#     datasource_tree = load_data_sources()
#     
#     # construct operation source tree
#     operationsources = OperationSource.query.all()
#     operation_tree = { 'name' : ('operations', ''), 'children' : [] }
#     for ops in operationsources:
#         operation_tree['children'].append({ 'name' : (ops.name, ops.id), 'children' : [] })
#         for op in ops.operations:
#             operation_tree['children'][-1]['children'].append({ 'name' : (op.name, op.id), 'children' : [] })
#     
#     # workflows tree
#     workflows = []
#     if current_user.is_authenticated:
#         #workflows = Workflow.query.filter_by(user_id=current_user.id)
#         #sql = 'SELECT workflows.*, MAX(time), taskstatus.name AS status FROM workflows JOIN users ON workflows.user_id = users.id LEFT JOIN workitems ON workflows.id = workitems.workflow_id LEFT JOIN tasks ON workitems.id = tasks.workitem_id LEFT JOIN tasklogs ON tasks.id=tasklogs.task_id JOIN taskstatus ON tasklogs.status_id=taskstatus.id GROUP BY workflows.id HAVING users.id=' + str(current_user.id)
#         sql = 'SELECT workflows.*, MAX(time), taskstatus.name AS status FROM workflows LEFT JOIN workitems ON workflows.id = workitems.workflow_id LEFT JOIN tasks ON workitems.id = tasks.workitem_id LEFT JOIN tasklogs ON tasks.id=tasklogs.task_id LEFT JOIN taskstatus ON tasklogs.status_id=taskstatus.id WHERE workflows.user_id={0} GROUP BY workflows.id'.format(current_user.id)
#         workflows = db.engine.execute(sql)
#     
#     workitems = []
# #    Workflow.query.join(WorkItem).join(Operation).filter_by(id=1).c
# #    sql = text('SELECT workitems.*, operations.name AS opname FROM workflows INNER JOIN workitems ON workflows.id=workitems.workflow_id INNER join operations ON workitems.operation_id=operations.id WHERE workflows.id=' + str(id))
#     
#     workflow_name = ''
#     if id is not None and Workflow.query.get(id) is not None:
#         workflow_name = Workflow.query.get(id).name
# #        sql = text('SELECT workitems.*, operations.name AS opname, datasources.id AS datasource_id, datasources.name AS datasource_name, data.url AS path FROM workflows INNER JOIN workitems ON workflows.id=workitems.workflow_id INNER join operations ON workitems.operation_id=operations.id INNER JOIN data ON workitems.id = data.id INNER JOIN datasources ON data.datasource_id=datasources.id WHERE workflows.id=' + str(id))
# #        sql = text('SELECT s.name AS name, s.input AS input, s.output AS output, dx.url AS input_root, dx2.url AS output_root, dx.type AS input_type, dx2.type AS output_type, operations.name AS opname FROM (SELECT w.*, d1.datasource_id AS input_datasource, d1.url AS input, d2.datasource_id AS output_datasource, d2.url AS output FROM workitems w INNER JOIN data d1 ON d1.id=w.input_id INNER JOIN data d2 ON d2.id=w.output_id) s INNER JOIN datasources dx ON dx.id=s.input_datasource INNER JOIN datasources dx2 ON dx2.id=s.output_datasource INNER JOIN operations ON s.operation_id = operations.id INNER JOIN workflows ON s.workflow_id=workflows.id WHERE workflows.id=' + str(id))
# #        sql = text('SELECT s.id AS id, s.name AS name, s.input AS input, s.output AS output, dx.url AS input_root, dx2.url AS output_root, dx.type AS input_type, dx2.type AS output_type, operations.name AS opname FROM (SELECT w.*, d1.datasource_id AS input_datasource, d1.url AS input, d2.datasource_id AS output_datasource, d2.url AS output FROM workitems w LEFT JOIN data d1 ON d1.id=w.input_id LEFT JOIN data d2 ON d2.id=w.output_id) s LEFT JOIN datasources dx ON dx.id=s.input_datasource LEFT JOIN datasources dx2 ON dx2.id=s.output_datasource LEFT JOIN operations ON s.operation_id = operations.id INNER JOIN workflows ON s.workflow_id=workflows.id WHERE workflows.id=' + str(id))
#         sql = text('SELECT w.id AS id, w.name AS name, w.desc as desc, ops.name AS opsname, operations.name AS opname, d1.url AS input, d2.url AS output, dx1.id AS input_datasourceid, dx1.type AS input_datasource, dx1.url AS input_root, dx2.id AS output_datasourceid, dx2.type AS output_datasource, dx2.url AS output_root FROM workitems w LEFT JOIN operations ON w.operation_id=operations.id LEFT JOIN operationsources ops ON ops.id=operations.operationsource_id LEFT JOIN data d1 ON d1.id=w.input_id LEFT JOIN data d2 ON d2.id=w.output_id LEFT JOIN datasources dx1 ON dx1.id=d1.datasource_id LEFT JOIN datasources dx2 ON dx2.id=d2.datasource_id WHERE w.workflow_id=' + str(id))
#         workitems = db.engine.execute(sql)        
# #         result = db.engine.execute(sql)
# #         for row in result:
# #             workitems.append(row);
#         
# #     if id is not None:
# #         workflow = Workflow.query.filter_by(id=id)
# #         if workflow is not None and workflow.count() > 0:
# #             workitems = workflow.first().workitems
#     
#     
#     return render_template('index.html', form=form, posts=posts, datasources=datasource_tree, operations=operation_tree, workflow=workflow_name, workflows=workflows, workitems=workitems,
#                            show_followed=show_followed, pagination=pagination)


@main.route('/user/<username>')
def user(username):
    user = User.query.filter_by(username=username).first_or_404()
    page = request.args.get('page', 1, type=int)
    pagination = user.posts.order_by(Post.timestamp.desc()).paginate(
        page, per_page=current_app.config['PHENOPROC_POSTS_PER_PAGE'],
        error_out=False)
    posts = pagination.items
    return render_template('user.html', user=user, posts=posts,
                           pagination=pagination)


@main.route('/edit-profile', methods=['GET', 'POST'])
@login_required
def edit_profile():
    form = EditProfileForm()
    if form.validate_on_submit():
        current_user.name = form.name.data
        current_user.location = form.location.data
        current_user.about_me = form.about_me.data
        db.session.add(current_user)
        flash('Your profile has been updated.')
        return redirect(url_for('.user', username=current_user.username))
    form.name.data = current_user.name
    form.location.data = current_user.location
    form.about_me.data = current_user.about_me
    return render_template('edit_profile.html', form=form)


@main.route('/edit-profile/<int:id>', methods=['GET', 'POST'])
@login_required
@admin_required
def edit_profile_admin(id):
    user = User.query.get_or_404(id)
    form = EditProfileAdminForm(user=user)
    if form.validate_on_submit():
        user.email = form.email.data
        user.username = form.username.data
        user.confirmed = form.confirmed.data
        user.role = Role.query.get(form.role.data)
        user.name = form.name.data
        user.location = form.location.data
        user.about_me = form.about_me.data
        db.session.add(user)
        flash('The profile has been updated.')
        return redirect(url_for('.user', username=user.username))
    form.email.data = user.email
    form.username.data = user.username
    form.confirmed.data = user.confirmed
    form.role.data = user.role_id
    form.name.data = user.name
    form.location.data = user.location
    form.about_me.data = user.about_me
    return render_template('edit_profile.html', form=form, user=user)


@main.route('/post/<int:id>', methods=['GET', 'POST'])
def post(id):
    post = Post.query.get_or_404(id)
    form = CommentForm()
    if form.validate_on_submit():
        comment = Comment(body=form.body.data,
                          post=post,
                          author=current_user._get_current_object())
        db.session.add(comment)
        flash('Your comment has been published.')
        return redirect(url_for('.post', id=post.id, page=-1))
    page = request.args.get('page', 1, type=int)
    if page == -1:
        page = (post.comments.count() - 1) // \
            current_app.config['PHENOPROC_COMMENTS_PER_PAGE'] + 1
    pagination = post.comments.order_by(Comment.timestamp.asc()).paginate(
        page, per_page=current_app.config['PHENOPROC_COMMENTS_PER_PAGE'],
        error_out=False)
    comments = pagination.items
    return render_template('post.html', posts=[post], form=form,
                           comments=comments, pagination=pagination)

@main.route('/workflow/<int:id>', methods=['GET', 'POST'])
def workflow(id):
    workflow = Workflow.query.get_or_404(id)
    return render_template('workflow.html', workflows=[workflow])
                           
@main.route('/edit/<int:id>', methods=['GET', 'POST'])
@login_required
def edit(id):
    post = Post.query.get_or_404(id)
    if current_user != post.author and \
            not current_user.can(Permission.ADMINISTER):
        abort(403)
    form = PostForm()
    if form.validate_on_submit():
        post.body = form.body.data
        db.session.add(post)
        flash('The post has been updated.')
        return redirect(url_for('.post', id=post.id))
    form.body.data = post.body
    return render_template('edit_post.html', form=form)


@main.route('/follow/<username>')
@login_required
@permission_required(Permission.FOLLOW)
def follow(username):
    user = User.query.filter_by(username=username).first()
    if user is None:
        flash('Invalid user.')
        return redirect(url_for('.index'))
    if current_user.is_following(user):
        flash('You are already following this user.')
        return redirect(url_for('.user', username=username))
    current_user.follow(user)
    flash('You are now following %s.' % username)
    return redirect(url_for('.user', username=username))


@main.route('/unfollow/<username>')
@login_required
@permission_required(Permission.FOLLOW)
def unfollow(username):
    user = User.query.filter_by(username=username).first()
    if user is None:
        flash('Invalid user.')
        return redirect(url_for('.index'))
    if not current_user.is_following(user):
        flash('You are not following this user.')
        return redirect(url_for('.user', username=username))
    current_user.unfollow(user)
    flash('You are not following %s anymore.' % username)
    return redirect(url_for('.user', username=username))


@main.route('/followers/<username>')
def followers(username):
    user = User.query.filter_by(username=username).first()
    if user is None:
        flash('Invalid user.')
        return redirect(url_for('.index'))
    page = request.args.get('page', 1, type=int)
    pagination = user.followers.paginate(
        page, per_page=current_app.config['PHENOPROC_FOLLOWERS_PER_PAGE'],
        error_out=False)
    follows = [{'user': item.follower, 'timestamp': item.timestamp}
               for item in pagination.items]
    return render_template('followers.html', user=user, title="Followers of",
                           endpoint='.followers', pagination=pagination,
                           follows=follows)


@main.route('/followed-by/<username>')
def followed_by(username):
    user = User.query.filter_by(username=username).first()
    if user is None:
        flash('Invalid user.')
        return redirect(url_for('.index'))
    page = request.args.get('page', 1, type=int)
    pagination = user.followed.paginate(
        page, per_page=current_app.config['PHENOPROC_FOLLOWERS_PER_PAGE'],
        error_out=False)
    follows = [{'user': item.followed, 'timestamp': item.timestamp}
               for item in pagination.items]
    return render_template('followers.html', user=user, title="Followed by",
                           endpoint='.followed_by', pagination=pagination,
                           follows=follows)


@main.route('/all')
@login_required
def show_all():
    resp = make_response(redirect(url_for('.index')))
    resp.set_cookie('show_followed', '', max_age=30*24*60*60)
    return resp


@main.route('/followed')
@login_required
def show_followed():
    resp = make_response(redirect(url_for('.index')))
    resp.set_cookie('show_followed', '1', max_age=30*24*60*60)
    return resp


@main.route('/moderate')
@login_required
@permission_required(Permission.MODERATE_COMMENTS)
def moderate():
    page = request.args.get('page', 1, type=int)
    pagination = Comment.query.order_by(Comment.timestamp.desc()).paginate(
        page, per_page=current_app.config['PHENOPROC_COMMENTS_PER_PAGE'],
        error_out=False)
    comments = pagination.items
    return render_template('moderate.html', comments=comments,
                           pagination=pagination, page=page)


@main.route('/moderate/enable/<int:id>')
@login_required
@permission_required(Permission.MODERATE_COMMENTS)
def moderate_enable(id):
    comment = Comment.query.get_or_404(id)
    comment.disabled = False
    db.session.add(comment)
    return redirect(url_for('.moderate',
                            page=request.args.get('page', 1, type=int)))


@main.route('/moderate/disable/<int:id>')
@login_required
@permission_required(Permission.MODERATE_COMMENTS)
def moderate_disable(id):
    comment = Comment.query.get_or_404(id)
    comment.disabled = True
    db.session.add(comment)
    return redirect(url_for('.moderate',
                            page=request.args.get('page', 1, type=int)))

@main.route('/about')
def about():
    return render_template('about.html')

@main.route('/contact')
def contact():
    return render_template('contact.html')

from sqlalchemy.ext.declarative import DeclarativeMeta
class AlchemyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj.__class__, DeclarativeMeta):
            # an SQLAlchemy class
            fields = {}
            for field in [x for x in dir(obj) if not x.startswith('_') and x != 'metadata']:
                data = obj.__getattribute__(field)
                try:
                    json.dumps(data) # this will fail on non-encodable values, like other classes
                    fields[field] = data
                except TypeError:
                    fields[field] = None
            # a json-encodable dict
            return fields
    
        return json.JSONEncoder.default(self, obj)

@main.route('/tasklogs', methods=['POST'])
@login_required
def translate():
    workflow_id = request.form['text'] #request.args.get('workflow_id')
    workflow_id = Utility.ValueOrNone(workflow_id)
    if workflow_id is not None and Workflow.query.get(workflow_id) is not None:
        sql = 'SELECT workitems.id, MAX(time), taskstatus.name as status FROM workitems LEFT JOIN tasks ON workitems.id=tasks.workitem_id LEFT JOIN tasklogs ON tasklogs.task_id=tasks.id LEFT JOIN taskstatus ON tasklogs.status_id = taskstatus.id WHERE workitems.workflow_id=' + str(workflow_id) + ' GROUP BY workitems.id'
        result = db.engine.execute(sql)
        return json.dumps([dict(r) for r in result], cls=AlchemyEncoder)

@main.route('/delete', methods=['POST'])
@login_required
def delete():
    datasource_id = Utility.ValueOrNone(request.form['datasource'])
    filesystem = getFileSystem(datasource_id)
    if filesystem is not None:
        path = os.path.join(Utility.get_rootdir(datasource_id), request.form['path'])
        filesystem.delete(path)
    return json.dumps(dict())

@main.route('/rename', methods=['POST'])
@login_required
def rename():
    datasource_id = Utility.ValueOrNone(request.form['datasource'])
    filesystem = getFileSystem(datasource_id)
    if filesystem is not None:
        oldpath = os.path.join(Utility.get_rootdir(datasource_id), request.form['path'])
        
        newpath = os.path.join(os.path.dirname(oldpath), request.form['newname'])
        filesystem.rename(oldpath, newpath)
        return json.dumps(filesystem.make_json(datasource_id, Utility.get_rootdir(datasource_id), os.path.relpath(newpath, Utility.get_rootdir(datasource_id))))
    return json.dumps(dict())
       
@main.route('/addfolder', methods=['POST'])
@login_required
def addfolder():
    datasource_id = Utility.ValueOrNone(request.form['datasource'])
    filesystem = getFileSystem(datasource_id)
    if filesystem is not None:
        path = os.path.join(Utility.get_rootdir(datasource_id), request.form['path'])
        newfolder = filesystem.addfolder(path)
        return json.dumps(filesystem.make_json(datasource_id, Utility.get_rootdir(datasource_id), os.path.relpath(newfolder, Utility.get_rootdir(datasource_id))))
    return json.dumps(dict())
    
    # Route that will process the file upload
@main.route('/upload', methods=['POST'])
def upload():
    # Get the name of the uploaded file
    file = request.files['file']
    
    # Check if the file is one of the allowed types/extensions
    if file:
        datasource_id = Utility.ValueOrNone(request.form['datasource'])
        filesystem = getFileSystem(datasource_id)
        if filesystem is not None:
            # Make the filename safe, remove unsupported chars
            filename = secure_filename(file.filename)
            # Move the file form the temporal folder to
            # the upload folder we setup
            path = os.path.join(Utility.get_rootdir(datasource_id), request.form['path'], filename)         
            filesystem.saveUpload(file, path)
            
    return json.dumps({})

@main.route('/download', methods=['POST'])
@login_required
def download():
    datasource_id = Utility.ValueOrNone(request.form['datasource'])
    filesystem = getFileSystem(datasource_id)
    if filesystem is not None:
        path = filesystem.download(os.path.join(Utility.get_rootdir(datasource_id), request.form['path']))
        if path is not None:
#             filename, ext = os.path.splitext(path)
#             if ext in mimetypes.types_map:
#                 mime = mimetypes.types_map[ext]
#             
#             if mime is None:
#                 try:
#                     mimetypes = mimetypes.read_mime_types(path)
#                     if mimetypes:
#                         mime = list(mimetypes.values())[0]
#                 except:
#                     pass
#             if mime is not None:
                    
            return send_from_directory(directory=os.path.dirname(path), filename=os.path.basename(path))
    return json.dumps(dict())

def load_data_sources_biowl():
        # construct data source tree
    datasources = DataSource.query.all()
    datasource_tree = []
    for ds in datasources:
        datasource = { 'path': ds.url, 'text': ds.name, 'nodes': [], 'folder': True}
        if ds.id == 1:
            # hdfs tree
            try:
                hdfs = HadoopFileSystem(ds.url, 'hdfs')
                if current_user.is_authenticated:
                    datasource['nodes'].append(hdfs.make_json(ds.id, Utility.get_rootdir(ds.id), current_user.username))
                datasource['nodes'].append(hdfs.make_json(ds.id, Utility.get_rootdir(ds.id), current_app.config['PUBLIC_DIR']))
            except:
                pass

        elif ds.id == 2:
            # file system tree
            posixFS = PosixFileSystem(Utility.get_rootdir(ds.id))
            if current_user.is_authenticated:
                datasource['nodes'].append(posixFS.make_json(current_user.username))
            datasource['nodes'].append(posixFS.make_json(current_app.config['PUBLIC_DIR']))
#         elif ds.id == 3:
#             # file system tree
#             if current_user.is_authenticated:
#                 try:
#                     galaxyFS = GalaxyFileSystem(ds.url, '7483fa940d53add053903042c39f853a')
#                     nodes = galaxyFS.make_json('/')
#                     if isinstance(nodes, list):
#                         datasource['nodes'] = nodes
#                     else:
#                         datasource['nodes'].append(nodes)
#                 except:
#                     pass
 
        datasource_tree.append(datasource)
        
    return datasource_tree

def download_biowl(path):
    # construct data source tree
    fs = Utility.fs_by_prefix(path)
    fullpath = fs.download(path)
    mime = mimetypes.guess_type(fullpath)[0]
    return send_from_directory(os.path.dirname(fullpath), os.path.basename(fullpath), mimetype=mime, as_attachment = mime is None )

def upload_biowl(file, fullpath):
    fs = Utility.fs_by_prefix(fullpath)
    return fs.save_upload(file, fullpath)
                
class InterpreterHelper():

    def __init__(self):
        self.funcs = []
        self.interpreter = Interpreter()
        self.codeGenerator = CodeGenerator()
        self.reload()
    
    def reload(self):
        self.funcs.clear()
        librariesdir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../biowl/libraries')
        librariesdir = os.path.normpath(librariesdir)
        self.interpreter.context.load_library(librariesdir)
        funclist = []
        for f in self.interpreter.context.library.funcs.values():
            funclist.extend(f)
        
        funclist.sort(key=lambda x: (x.group, x.name))
        for f in funclist:
            self.funcs.append({"package_name": f.package if f.package else "", "name": f.name, "internal": f.internal, "example": f.example if f.example else "", "desc": f.desc if f.desc else "", "runmode": f.runmode if f.runmode else "", "level": f.level, "group": f.group if f.group else ""}) 
    
        self.codeGenerator.context.load_library(librariesdir)
        
    def run(self, machine, script):
        parserdir = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../biowl/'))
        os.chdir(parserdir) #set dir of this file to current directory
        duration = 0
        try:
            machine.context.reload()
            parser = PhenoWLParser(PythonGrammar())   
            with Timer() as t:
                prog = parser.parse(script)
                machine.run(prog)
            duration = t.secs
        except:
            machine.context.err.append("Error in parse and interpretation")
        return { 'out': machine.context.out, 'err': machine.context.err, 'duration': "{:.4f}s".format(duration) }

interpreter = InterpreterHelper()
    
@main.route('/biowl', methods=['GET', 'POST'])
@main.route('/', methods = ['GET', 'POST'])
#@login_required
def index():
    return render_template('biowl.html')
 
@main.route('/datasources', methods=['GET', 'POST'])
@login_required
def datasources():
    if request.form.get('download'):
        return download_biowl(request.form['download'])
    elif request.files and request.files['upload']:
        upload_biowl(request.files['upload'], request.form['path'])
    elif request.args.get('addfolder'):
        path = request.args['addfolder']
        fileSystem = Utility.fs_by_prefix(path)
        parent = path if fileSystem.isdir(path) else os.path.dirname(path)
        unique_filename = IOHelper.unique_fs_name(fileSystem, parent, 'newfolder', '')
        return json.dumps({'path' : fileSystem.create_folder(unique_filename) })
    elif request.args.get('delete'):
        path = request.args['delete']
        fileSystem = Utility.fs_by_prefix(path)
        return json.dumps({'path' : fileSystem.remove(fileSystem.strip_root(path))})
    elif request.args.get('rename'):
        fileSystem = Utility.fs_by_prefix(request.args['oldpath'])
        oldpath = fileSystem.strip_root(request.args['oldpath'])
        newpath = os.path.join(os.path.dirname(oldpath), request.args['rename'])
        return json.dumps({'path' : fileSystem.rename(oldpath, newpath)})
        
    return json.dumps({'datasources': load_data_sources_biowl() })

def run_biowl(user_id, script, args, immediate = True, pygen = False):
    machine = interpreter.codeGenerator if pygen else interpreter.interpreter
                
    runnable_id = Runnable.create_runnable(user_id)
    runnable = Runnable.query.get(runnable_id)
    runnable.script = script
    runnable.name = script[:min(40, len(script))]
    if len(script) > len(runnable.name):
        runnable.name += "..."
    db.session.commit()
    
    if immediate:
        try:
            runnable.status = Status.STARTED
            db.session.commit()
            
            result = run_script(machine, script, args)
            runnable = Runnable.query.get(runnable_id)
            if result:
                runnable.status = Status.FAILURE if result['err'] else Status.SUCCESS
                runnable.out = "\n".join(result['out'])
                runnable.err = "\n".join(result['err'])
            else:
                runnable.status = Status.FAILURE
                
            runnable.update()
        except:
            result = {}
            
        return json.dumps(result)
    else:
        task = run_script.delay(machine, script, args)
        runnable.celery_id = task.id
        runnable.update()
        return json.dumps({})

import pip

def install(package):
    pip.main(['install', package])
        
def get_functions(level):
    funcs =[func for func in interpreter.funcs if int(func['level']) <= level]
    return json.dumps({'functions':  funcs})
    
@main.route('/functions', methods=['GET', 'POST'])
@login_required
def functions():
    if request.method == "POST":
        if request.form.get('script') or request.form.get('code'):
            script = request.form.get('script') if request.form.get('script') else request.form.get('code')
            args = request.form.get('args') if request.form.get('args') else ''
            immediate = request.form.get('immediate') == 'true'.lower() if request.form.get('immediate') else False
            pygen = True if request.form.get('code') else False
            return run_biowl(current_user.id, script, args, immediate, pygen)
        
        elif request.form.get('mapper'):
            result = {"out": [], "err": []}
            try:
                if request.form.get('pip'):
                    pippkgs = request.form.get('pip')
                    pippkgs = pippkgs.split(",")
                    for pkg in pippkgs:
                        try:
                            install(pkg)
                        except Exception as e:
                            result['err'].append(e)
                    
                # Get the name of the uploaded file
                file = request.files['library']
                # Check if the file is one of the allowed types/extensions
                if file:
                    this_path = os.path.dirname(os.path.abspath(__file__))
                    #os.chdir(this_path) #set dir of this file to current directory
                    app_path = os.path.dirname(this_path)
                    librariesdir = os.path.normpath(os.path.join(app_path, 'biowl/libraries'))

                    user_package_dir = os.path.normpath(os.path.join(librariesdir, 'users', current_user.username))
                    if not os.path.isdir(user_package_dir):
                        os.makedirs(user_package_dir)
                    package = request.form.get('package')
                    path = Samples.unique_filename(user_package_dir, package if package else 'mylib', '')
                    if not os.path.isdir(path):
                        os.makedirs(path)
                
                    # Make the filename safe, remove unsupported chars
                    filename = secure_filename(file.filename)
                    temppath = os.path.join(tempfile.gettempdir(), filename)
                    file.save(temppath)
                    
                    if zipfile.is_zipfile(temppath):
                        with zipfile.ZipFile(temppath,"r") as zip_ref:
                            zip_ref.extractall(path)
                    elif tarfile.is_tarfile(temppath):
                        with tarfile.open(temppath,"r") as tar_ref:
                            def is_within_directory(directory, target):
                                
                                abs_directory = os.path.abspath(directory)
                                abs_target = os.path.abspath(target)
                            
                                prefix = os.path.commonprefix([abs_directory, abs_target])
                                
                                return prefix == abs_directory
                            
                            def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
                            
                                for member in tar.getmembers():
                                    member_path = os.path.join(path, member.name)
                                    if not is_within_directory(path, member_path):
                                        raise Exception("Attempted Path Traversal in Tar File")
                            
                                tar.extractall(path, members, numeric_owner=numeric_owner) 
                                
                            
                            safe_extract(tar_ref, path)
                    else:
                        shutil.move(temppath, path)
                    
                    base = Samples.unique_filename(path, package if package else 'mylib', '.json')
                    with open(base, 'w') as mapper:
                        mapper.write(request.form.get('mapper'))
                    
                    org = request.form.get('org')
                    pkgpath = str(pathlib.Path(path).relative_to(os.path.dirname(app_path)))
                    pkgpath = pkgpath.replace(os.sep, '.')

                    with open(base, 'r') as json_data:
                        data = json.load(json_data)
                        libraries = data["functions"]
                        for f in libraries:
                            if 'internal' in f and f['internal']:
                                if 'name' not in f:
                                    f['name'] = f['internal']
                            elif 'name' in f and f['name']:
                                if 'internal' not in f:
                                    f['internal'] = f['name']
                            if not f['internal'] and not f['name']:
                                continue
                                    
                            f['module'] = pkgpath
                            if package:
                                f['package'] = package
                            if org:
                                f['org'] = org
                                
                    os.remove(base)
                    with open(base, 'w') as f:
                        json.dump(data, f, indent=4)
                    interpreter.reload()
                    
                    result['out'].append("Library successfully added.")
            except Exception as e:
                result['err'].append(e)
            return json.dumps(result)
        elif request.form.get('provenance'):
            fullpath = os.path.join(os.path.dirname(os.path.dirname(basedir)), "workflow.log")
            mime = mimetypes.guess_type(fullpath)[0]
            return send_from_directory(os.path.dirname(fullpath), os.path.basename(fullpath), mimetype=mime, as_attachment = mime is None )
    else:
        level = int(request.args.get('level')) if request.args.get('level') else 0
        return get_functions(level)
    
class Samples():
    @staticmethod
    def load_samples_recursive(library_def_file):
        if os.path.isfile(library_def_file):
            return Samples.load_samples(library_def_file)
        
        all_samples = []
        for f in os.listdir(library_def_file):
            samples = Samples.load_samples_recursive(os.path.join(library_def_file, f))
            all_samples.extend(samples if isinstance(samples, list) else [samples])
            #all_samples = {**all_samples, **samples}
        return all_samples
       
    @staticmethod
    def load_samples(sample_def_file):
        samples = []
        if not os.path.isfile(sample_def_file) or not sample_def_file.endswith(".json"):
            return samples
        try:
            with open(sample_def_file, 'r') as json_data:
                d = json.load(json_data)
                samples = d["samples"] if d.get("samples") else d 
        finally:
            return samples
        
    @staticmethod
    def get_samples_as_list():
        samples = []
        samplesdir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../biowl/samples')
        for s in Samples.load_samples_recursive(samplesdir):
            samples.append({"name": s["name"], "desc": s["desc"], "sample": '\n'.join(s["sample"])})
        return samples
    
    @staticmethod
    def make_fn(path, prefix, ext, suffix):
        path = os.path.join(path, '{0}'.format(prefix))
        if suffix:
            path = '{0}({1})'.format(path, suffix)
        if ext:
            path = '{0}.{1}'.format(path, ext)
        return path
    
    @staticmethod
    def unique_filename(path, prefix, ext):
        uni_fn = Samples.make_fn(path, prefix, ext, '')
        if not os.path.exists(uni_fn):
            return uni_fn
        for i in range(1, sys.maxsize):
            uni_fn = Samples.make_fn(path, prefix, ext, i)
            if not os.path.exists(uni_fn):
                return uni_fn
            
    @staticmethod
    def add_sample(sample, name, desc):
        this_path = os.path.dirname(os.path.abspath(__file__))
        os.chdir(this_path) #set dir of this file to current directory
        samplesdir = os.path.normpath(os.path.join(this_path, '../biowl/samples'))

        try:
            if sample and name:
                new_path = os.path.normpath(os.path.join(samplesdir, 'users', current_user.username))
                if not os.path.isdir(new_path):
                    os.makedirs(new_path)
                path = Samples.unique_filename(new_path, 'sample', 'json')
                
                with open(path, 'w') as fp:
                    fp.write("{\n")
                    fp.write('{0}"name":"{1}",\n'.format(" " * 4, name));
                    fp.write('{0}"desc":"{1}",\n'.format(" " * 4, desc));
                    fp.write('{0}"sample":[\n'.format(" " * 4))
                    sample = sample.replace("\\n", "\n").replace("\r\n", "\n").replace("\"", "\'")
                    lines = sample.split("\n")
                    for line in lines[0:-1]:
                        fp.write('{0}"{1}",\n'.format(" " * 8, line))
                    fp.write('{0}"{1}"\n'.format(" " * 8, lines[-1]))
                    fp.write("{0}]\n}}".format(" " * 4))
#                json.dump(samples, fp, indent=4, separators=(',', ': '))
        finally:
            return { 'out': '', 'err': ''}, 201
    
    
@main.route('/samples', methods=['GET', 'POST'])
@login_required
def samples():
    if request.form.get('sample'):
        return Samples.add_sample(request.form.get('sample'), request.form.get('name'), request.form.get('desc'))
    return json.dumps({'samples': Samples.get_samples_as_list()})

def get_user_status(user_id):
    return jsonify(runnables =[i.to_json() for i in Runnable.query.filter(Runnable.user_id == user_id)])

def get_task_status(task_id):
    runnable = Runnable.query.get(task_id)
    return jsonify(runnable = runnable.to_json())
    
@main.route('/runnables', methods=['GET', 'POST'])
@login_required
def runnables():
    if request.args.get('id'):
        return get_task_status(int(request.args.get('task_id')))
    elif request.args.get('stop'):
        ids = request.args.get('stop')
        ids = ids.split(",")
        new_status = []
        for id in ids:
            runnable = Runnable.query.get(int(id))
            stop_script(runnable.celery_id)
            sync_task_status_with_db(runnable)
            new_status.append(runnable)
        return jsonify(runnables =[i.to_json() for i in new_status])
    
    sync_task_status_with_db_for_user(current_user.id)
#     runnables_db = Runnable.query.filter(Runnable.user_id == current_user.id)
#     rs = []
#     for r in runnables_db:
#       rs.append(r.to_json())
#     return jsonify(runnables = rs)
    return get_user_status(current_user.id)