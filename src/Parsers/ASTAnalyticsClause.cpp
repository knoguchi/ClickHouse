#include <Parsers/ASTAnalyticsClause.h>
#include <IO/WriteBuffer.h>
#include <Parsers/ASTExpressionList.h>

namespace DB
{

ASTPtr ASTAnalyticsClause::clone() const
{
    auto clone = std::make_shared<ASTAnalyticsClause>(*this);
    clone->cloneChildren();
    return clone;
}

//void ASTAnalyticsClause::appendColumnName(WriteBuffer & ostr) const { ostr.write('*'); }

void ASTAnalyticsClause::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
//    frame.current_select = this;
//    frame.need_parens = true;
    frame.expression_list_prepend_whitespace = true; // adds space between ORDER BY and expressions

    std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');

    settings.ostr << "OVER(";

    if (partition_expression_list)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << indent_str << "PARTITION BY" << (settings.hilite ? hilite_none : "");
        settings.one_line
        ? partition_expression_list->formatImpl(settings, state, frame)
        : partition_expression_list->as<ASTExpressionList &>().formatImplMultiline(settings, state, frame);
    }

    if (order_expression_list)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << indent_str << "ORDER BY" << (settings.hilite ? hilite_none : "");
        settings.one_line
        ? order_expression_list->formatImpl(settings, state, frame)
        : order_expression_list->as<ASTExpressionList &>().formatImplMultiline(settings, state, frame);
    }

    for (const auto & child : children)
    {
        settings.ostr << ' ';
        child->formatImpl(settings, state, frame);
    }

    settings.ostr << ")";

}

}
