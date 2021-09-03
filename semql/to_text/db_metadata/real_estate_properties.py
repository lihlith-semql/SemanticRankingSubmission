
from semql.to_text.attribute import *
from semql.to_text.table import *

REAL_ESTATE_PROPS = {
    "tables": {
        "ref_feature_types": Table(
            name="ref_feature_types",
            pretty_name="feature type",
            attributes={
                "feature_type_code": PrimaryKey(),
                "feature_type_name": Name(is_default=True),
            }
        ),
        "ref_property_types": Table(
            name="ref_property_types",
            pretty_name="property type",
            attributes={
                "property_type_code": PrimaryKey(),
                "property_type_description": Text(name="description", is_default=True),
            }
        ),
        "other_available_features": PartialRelation(
            name="other_available_features",
            pretty_name="other feature",
            attributes={
                "feature_id": PrimaryKey(),
                "feature_type_code": ForeignKey(name="feature_type_code", target_table="ref_feature_types"),
                "feature_name": Name(is_default=True),
                "feature_description": Text(name="description"),
            },
            templates={
                "feature_type_code": {
                    "ref_feature_types": ["$ref_feature_types", "of", "$other_available_features"],
                    "other_available_features": ["$other_available_features", "with", "$ref_feature_types"],
                }
            }
        ),
        "properties": PartialRelation(
            name="properties",
            pretty_name="property",
            attributes={
                "property_id": PrimaryKey(),
                "property_type_code": ForeignKey(name="property_type_code", target_table="ref_property_types"),
                "date_on_market": Date(name="date on market", participle="put on the market", preposition="on"),
                "date_sold": Date(name="sale date", participle="sold", preposition="on"),
                "property_name": Name(is_default=True),
                "property_address": Text(name="address"),
                "room_count": Numeric(name="", unit="room"),
                "vendor_requested_price": Numeric(name="requested price"),
                "buyer_offered_price": Numeric(name="offered price"),
                "agreed_selling_price": Numeric(name="agreed selling price"),
                "apt_feature_1": Text(name="first apartment feature"),
                "apt_feature_2": Text(name="second apartment feature"),
                "apt_feature_3": Text(name="third apartment feature"),
                "fld_feature_1": Text(name="first fld feature"),
                "fld_feature_2": Text(name="second fld feature"),
                "fld_feature_3": Text(name="third fld feature"),
                "hse_feature_1": Text(name="first hse feature"),
                "hse_feature_2": Text(name="second hse feature"),
                "hse_feature_3": Text(name="third hse feature"),
                "oth_feature_1": Text(name="first other feature"),
                "oth_feature_2": Text(name="second other feature"),
                "oth_feature_3": Text(name="third other feature"),
                "shp_feature_1": Text(name="first shp feature"),
                "shp_feature_2": Text(name="second shp feature"),
                "shp_feature_3": Text(name="third shp feature"),
                "other_property_details": Text(name="other details"),
            },
            templates={
                "property_type_code": {
                    "properties": ["$properties", "of", "$ref_property_types"],
                    "ref_property_types": ["$ref_property_types", "of", "$properties"],
                }
            }
        ),
        "other_property_features": PartialRelation(
            name="other_property_features",
            pretty_name="other property feature",
            attributes={
                "property_id": ForeignKey(name="property_id", target_table="properties"),
                "feature_id": ForeignKey(name="feature_id", target_table="other_available_features"),
                "property_feature_description": Text(name="description"),
            },
            templates={
                "property_id": {
                    "properties": ["$properties", "with", "$other_property_features"],
                    "other_property_features": ["$other_property_features", "of", "$properties"],
                },
                "feature_id": {
                    "other_availabel_features": ["$other_available_features", "of", "$properties"],
                    "properties": ["$properties", "with", "$other_available_features"],
                },
            }
        )
    },
}
