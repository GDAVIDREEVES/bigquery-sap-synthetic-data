from acdoca_generator.config.industries import canonical_industry_key, get_industry


def test_consumer_products_alias_resolves():
    assert canonical_industry_key("consumer_products") == "consumer_goods"
    t = get_industry("consumer_products")
    assert t.key == "consumer_goods"
