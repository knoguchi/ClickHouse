#pragma once

#include <Parsers/IAST.h>
#include <common/types.h>

namespace DB
{
class ASTAnalyticsClause : public IAST
{
public:
    ASTPtr partition_expression_list;
    ASTPtr order_expression_list;
    ASTPtr windowing;

public:
    bool is_row = false;
    bool is_range = false;

    ASTAnalyticsClause() { }

    String getID(char) const override { return "analytics_clause"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

}
