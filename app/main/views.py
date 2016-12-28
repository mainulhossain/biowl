from __future__ import print_function

from flask import Flask, render_template, redirect, url_for, abort, flash, request,\
    current_app, make_response, g
from flask_login import login_required, current_user
from flask_sqlalchemy import get_debug_queries
from . import main
from .forms import EditProfileForm, EditProfileAdminForm, PostForm,\
    CommentForm
from .. import db
from ..models import Permission, Role, User, Post, Comment, Workflow, WorkItem, DataSource, OperationSource, Operation
from ..decorators import admin_required, permission_required
from sqlalchemy import text
import os
import sys
import flask_sijax
from ..operations import execute_workflow

try:
    from hdfs import InsecureClient
except:
    pass

app = Flask(__name__)

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

def make_fs_tree(path):
    #tree = dict(name=os.path.basename(path), children=[])
    tree = dict(name=(os.path.basename(path), path), children=[])
    try: lst = os.listdir(path)
    except OSError:
        pass #ignore errors
    else:
        for name in lst:
            fn = os.path.join(path, name)
            if os.path.isdir(fn):
                tree['children'].append(make_fs_tree(fn))
            else:
                tree['children'].append({'name' : (name, fn), 'children' : []})
    return tree

def make_hdfs_tree(client, path):
    tree = dict(name=(os.path.basename(path), path), children=[])
    try: lst = client.list(path, status=True)
    except:
        pass #ignore errors
    else:
        for fsitem in lst:
            fn = os.path.join(path, fsitem[0])
            if fsitem[1]['type'] == "DIRECTORY":
                tree['children'].append(make_hdfs_tree(client, fn))
            else:
                tree['children'].append({'name' : (fsitem[0], fn), 'children' : []})
    return tree

@main.route('/', defaults={'id': ''}, methods = ['GET', 'POST'])
@main.route('/workflow/<int:id>/', methods = ['GET', 'POST'])
def index(id=None):
    
    def ValueOrNone(val):
        try:
            return int(val)
        except ValueError:
            return 0
            
    id = ValueOrNone(id)
    if id <= 0:
        id = request.args.get('workflow')
        
    def run_workflow(obj_response, workflow_id):
        workflow_id = ValueOrNone(workflow_id)
        if workflow_id is not None and Workflow.query.get(workflow_id) is not None:
            execute_workflow(workflow_id)
            
    def set_editmode(obj_response, mode):
        current_app.config['WORKFLOW_MODE_EDIT'] = mode
    
    def add_workflow(obj_response):
        if current_user.is_authenticated:
            workflow = Workflow(user_id=current_user.id, name='New Workflow')            
            db.session.add(workflow)
            db.session.commit()
            obj_response.redirect(request.base_url + '{0}{1}'.format('?workflow=', workflow.id))
    
    def delete_workitem(obj_response, workitem_id):
        if current_user.is_authenticated:
            if workitem_id is not None and WorkItem.query.get(workitem_id) is not None:
                WorkItem.query.filter(WorkItem.id == workitem_id).delete()
                db.session.commit()
                
            
    def delete_workflow(obj_response, workflow_id):
        if current_user.is_authenticated:
            if workflow_id is not None and Workflow.query.get(workflow_id) is not None:
                Workflow.query.filter(Workflow.id == workflow_id).delete()
                db.session.commit()
                                    
    def add_workitem(obj_response, wf_id):
        if current_user.is_authenticated:
            wf_id = ValueOrNone(wf_id)
            print(str(wf_id), file=sys.stderr)
            if wf_id is not None and Workflow.query.get(wf_id) is not None:
                workitem = WorkItem(workflow_id=wf_id, name='New Workitem')            
                db.session.add(workitem)
                db.session.commit()
    
    def add_input(obj_response, workitem_id, data_id):
        if current_user.is_authenticated:
            workflow_id = ValueOrNone(workflow_id)
            if workflow_id is not None and Workflow.query.get(workflow_id) is not None:
                workitem = WorkItem(workflow_id=workflow_id, name='New Workitem')            
                db.session.add(workitem)
                db.session.commit()
                            
    if g.sijax.is_sijax_request:
        # Sijax request detected - let Sijax handle it
        g.sijax.register_callback('run_workflow', run_workflow)
        g.sijax.register_callback('set_editmode', set_editmode)
        g.sijax.register_callback('add_workflow', add_workflow)
        g.sijax.register_callback('add_workitem', add_workitem)
        return g.sijax.process_request()

    form = PostForm()
    if current_user.can(Permission.WRITE_ARTICLES) and form.validate_on_submit():
        post = Post(body=form.body.data, author=current_user._get_current_object())
        db.session.add(post)
        return redirect(url_for('.index'))
    page = request.args.get('page', 1, type=int)
    show_followed = False
    if current_user.is_authenticated:
        show_followed = bool(request.cookies.get('show_followed', ''))
    if show_followed:
        query = current_user.followed_posts
    else:
        query = Post.query
    pagination = query.order_by(Post.timestamp.desc()).paginate(
        page, per_page=current_app.config['PHENOPROC_POSTS_PER_PAGE'],
        error_out=False)
    posts = pagination.items
    
    # construct data source tree
    datasources = DataSource.query.all()
    datasource_tree = { 'name' : ('datasources', ''), 'children' : [] }
    for ds in datasources:
        datasource_tree['children'].append({ 'name' : (ds.name, ds.url), 'children' : [] })
    
    # hdfs tree         
    try:
        client = InsecureClient(current_app.config['WEBHDFS_ADDR'], user=current_app.config['WEBHDFS_USER'])
    except:
        pass
    else:
        hdfs_tree = datasource_tree['children'][0]['children']
        if client is not None:
            if current_user.is_authenticated:
                hdfs_tree.append(make_hdfs_tree(client, os.path.join(current_app.config['HDFS_DIR'], current_user.username)))
            hdfs_tree.append(make_hdfs_tree(client, os.path.join(current_app.config['HDFS_DIR'], 'public')))
    
    # file system tree
    fs_tree = datasource_tree['children'][1]['children']
    if current_user.is_authenticated and os.path.exists(os.path.join(current_app.config['DATA_DIR'], current_user.username)):
        fs_tree.append(make_fs_tree(os.path.join(current_app.config['DATA_DIR'], current_user.username)))
        
    fs_tree.append(make_fs_tree(os.path.join(current_app.config['DATA_DIR'], 'public')))
    
    # construct operation source tree
    operationsources = OperationSource.query.all()
    operation_tree = { 'name' : 'operations', 'children' : [] }
    for ops in operationsources:
        operation_tree['children'].append({ 'name' : ops.name, 'children' : [] })
        for op in ops.operations:
            operation_tree['children'][-1]['children'].append({ 'name' : op.name, 'children' : [] })
    
    # workflows tree
    workflows = []
    if current_user.is_authenticated:
        workflows = Workflow.query.filter_by(user_id=current_user.id)
    
    workitems = []
#    Workflow.query.join(WorkItem).join(Operation).filter_by(id=1).c
#    sql = text('SELECT workitems.*, operations.name AS opname FROM workflows INNER JOIN workitems ON workflows.id=workitems.workflow_id INNER join operations ON workitems.operation_id=operations.id WHERE workflows.id=' + str(id))
    
    if id is not None and Workflow.query.get(id) is not None:
#        sql = text('SELECT workitems.*, operations.name AS opname, datasources.id AS datasource_id, datasources.name AS datasource_name, data.url AS path FROM workflows INNER JOIN workitems ON workflows.id=workitems.workflow_id INNER join operations ON workitems.operation_id=operations.id INNER JOIN data ON workitems.id = data.id INNER JOIN datasources ON data.datasource_id=datasources.id WHERE workflows.id=' + str(id))
#        sql = text('SELECT s.name AS name, s.input AS input, s.output AS output, dx.url AS input_root, dx2.url AS output_root, dx.type AS input_type, dx2.type AS output_type, operations.name AS opname FROM (SELECT w.*, d1.datasource_id AS input_datasource, d1.url AS input, d2.datasource_id AS output_datasource, d2.url AS output FROM workitems w INNER JOIN data d1 ON d1.id=w.input_id INNER JOIN data d2 ON d2.id=w.output_id) s INNER JOIN datasources dx ON dx.id=s.input_datasource INNER JOIN datasources dx2 ON dx2.id=s.output_datasource INNER JOIN operations ON s.operation_id = operations.id INNER JOIN workflows ON s.workflow_id=workflows.id WHERE workflows.id=' + str(id))
        sql = text('SELECT s.name AS name, s.input AS input, s.output AS output, dx.url AS input_root, dx2.url AS output_root, dx.type AS input_type, dx2.type AS output_type, operations.name AS opname FROM (SELECT w.*, d1.datasource_id AS input_datasource, d1.url AS input, d2.datasource_id AS output_datasource, d2.url AS output FROM workitems w LEFT JOIN data d1 ON d1.id=w.input_id LEFT JOIN data d2 ON d2.id=w.output_id) s LEFT JOIN datasources dx ON dx.id=s.input_datasource LEFT JOIN datasources dx2 ON dx2.id=s.output_datasource LEFT JOIN operations ON s.operation_id = operations.id INNER JOIN workflows ON s.workflow_id=workflows.id WHERE workflows.id=' + str(id))        
        result = db.engine.execute(sql)
        for row in result:
            workitems.append(row);
#     if id is not None:
#         workflow = Workflow.query.filter_by(id=id)
#         if workflow is not None and workflow.count() > 0:
#             workitems = workflow.first().workitems
    
    
    return render_template('index.html', form=form, posts=posts, datasources=datasource_tree, operations=operation_tree, workflows=workflows, workitems=workitems,
                           show_followed=show_followed, pagination=pagination)


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