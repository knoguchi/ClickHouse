#include <Parsers/ASTAnalyticsClause.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserAnalyticsClause.h>

namespace DB
{
bool ParserAnalyticsClause::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto analytics_clause = std::make_shared<ASTAnalyticsClause>();
    node = analytics_clause;

    ParserKeyword s_partition_by("PARTITION BY");
    ParserExpressionList query_partition_list(false);
    ASTPtr partition_expression_list;

    ParserKeyword s_order_by("ORDER BY");
    ParserOrderByExpressionList order_list;
    ASTPtr order_expression_list;


    /// PARTITION BY expr list
    if (s_partition_by.ignore(pos, expected))
    {
        if (!query_partition_list.parse(pos, partition_expression_list, expected))
            return false;
        analytics_clause->partition_expression_list = partition_expression_list;
    }

    /// ORDER BY expr ASC|DESC COLLATE 'locale' list
    if (s_order_by.ignore(pos, expected))
    {
        if (!order_list.parse(pos, order_expression_list, expected))
            return false;
        analytics_clause->order_expression_list = order_expression_list;
    }

    return true;
}

}
