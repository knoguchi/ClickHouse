#include <Parsers/ASTFrameElement.h>
#include <Parsers/ASTWindowingElement.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserFrame.h>
#include <Parsers/ExpressionElementParsers.h>

namespace DB
{
/**
 * frame_clause := frame_units | frame_extent
 * frame_units := ROW | RANGE
 *     ROWS: The frame is defined by beginning and ending row positions. Offsets are differences in row numbers from the current row number.
 *     RANGE: The frame is defined by rows within a value range. Offsets are differences in row values from the current row value.
 * frame_extent:= {frame_start | frame_between}
 *
 * frame_start, frame_end := {
 *  CURRENT ROW
 *  UNBOUNDED PRECEDING
 *  UNBOUNDED FOLLOWING (ORACLE doesn't support)
 *  expr PRECEDING
 *  expr FOLLOWING  (ORACLE doesn't support)
 * }
 *
 * frame_between := BETWEEN frame_start AND frame_end
 *
 */
bool ParserFrame::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto frame_element = std::make_shared<ASTFrameElement>();
    node = frame_element;

    ParserKeyword s_unbounded_preceding("UNBOUNDED PRECEDING");
    ParserKeyword s_current_row("CURRENT ROW");
    ParserKeyword s_preceding("PRECEDING");
    ParserKeyword s_following("FOLLOWING");

    ParserExpressionElement expr_parser;
    ASTPtr expr;

    /// UNBOUNDED PRECEDING
    if (s_unbounded_preceding.ignore(pos, expected))
    {
        frame_element->unbounded_preceding = true;
    } else if(s_current_row.ignore(pos, expected))
    {
        frame_element->current_row = true;
    } else
    {
        // expr <PRECEDING | FOLLOWING>
        expr_parser.parse(pos, expr, expected);
        if(!expr)
            return false;
        frame_element->value = expr;

        frame_element->preceding = s_preceding.ignore(pos, expected);
        frame_element->following = s_following.ignore(pos, expected);
        if(!frame_element->preceding && !frame_element->following)
            return false;
    }
    return true;
}

}
