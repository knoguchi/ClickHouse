#include <Parsers/ASTAnalyticsClause.h>
#include <Parsers/ASTWindowingElement.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserWindowingElement.h>
#include <Parsers/ParserFrame.h>

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
bool ParserWindowingElement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto windowing = std::make_shared<ASTWindowingElement>();
    node = windowing;

    ParserKeyword s_between("BETWEEN");
    ParserKeyword s_and("AND");

    ParserFrame frame_parser;

    ASTPtr frame_start;
    ASTPtr frame_end;

    if (s_between.ignore(pos, expected))
    {
        frame_parser.parse(pos, frame_start, expected);
        if (!s_and.ignore(pos, expected))
            return false;
        frame_parser.parse(pos, frame_end, expected);

        windowing->frame_start = frame_start;
        windowing->frame_end = frame_end;
    }
    else
    {
        frame_parser.parse(pos, frame_start, expected);
        if(!frame_start)
            return false;
        windowing->frame_value = frame_start;
    }
    return true;
}

}
