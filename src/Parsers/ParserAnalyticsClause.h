#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/*
 [ query_partition_clause ] [ order_by_clause [ windowing_clause ] ]
 */
class ParserAnalyticsClause final : public IParserBase
{
protected:
    const char * getName() const override { return "analytics clause"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserQueryPartitionByExpressionList : public IParserBase
{
protected:
    const char * getName() const override { return "query partition by expression"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
