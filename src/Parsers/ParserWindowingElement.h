#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{
/*
 order_by_clause [ windowing_clause ]
 windowing_clause is also known as frame_clause in MySQL
 */
class ParserWindowingElement final : public IParserBase
{
protected:
    const char * getName() const override { return "windowing clause"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
