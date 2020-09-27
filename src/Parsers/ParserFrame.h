#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{
/*
    frame element is a part of windowing clause
 */
class ParserFrame final : public IParserBase
{
protected:
    const char * getName() const override { return "frame element"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
