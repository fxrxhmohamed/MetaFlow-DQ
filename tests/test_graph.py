from loading_service.graph import Step, build_graph


def test_build_graph_links_layers():
    steps, edges = build_graph(
        [{"id": "raw"}],
        [{"id": "trips", "bronze_id": "raw"}],
        [{"id": "fct", "depends_on": ["trips"]}],
    )
    assert [s.task_id for s in steps] == ["bronze__raw", "silver__trips", "gold__fct"]
    assert edges == [(Step("bronze", "raw"), Step("silver", "trips")),
                     (Step("silver", "trips"), Step("gold", "fct"))]
