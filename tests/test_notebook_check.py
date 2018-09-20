def test_warn_without_notebook_support():
    import radiantearth.decorators
    radiantearth.decorators.NOTEBOOK_SUPPORT = False
    from radiantearth.decorators import check_notebook

    @check_notebook
    def f():
        return 'foo'
    assert f() is None


def test_warn_without_notebook_support_with_args():
    import radiantearth.decorators
    radiantearth.decorators.NOTEBOOK_SUPPORT = False
    from radiantearth.decorators import check_notebook

    @check_notebook
    def f(*args, **kwargs):
        return 'foo'
    assert f(1, 2, 3, foo='bar') is None


def test_no_warn_with_notebook_support():
    import radiantearth.decorators
    radiantearth.decorators.NOTEBOOK_SUPPORT = True
    from radiantearth.decorators import check_notebook

    @check_notebook
    def f():
        return 'foo'
    assert f() == 'foo'
