#include <Parsers/ASTWindowingElement.h>
#include <Common/SipHash.h>


namespace DB
{

ASTPtr ASTWindowingElement::clone() const
{
    auto clone = std::make_shared<ASTWindowingElement>(*this);
    clone->cloneChildren();
    return clone;
}

void ASTWindowingElement::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if(frame_start && frame_end) {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " BETWEEN" << (settings.hilite ? hilite_none : "");
        frame_start->formatImpl(settings, state, frame);
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " AND" << (settings.hilite ? hilite_none : "");
        frame_end->formatImpl(settings, state, frame);
    } else {
        frame_value->formatImpl(settings, state, frame);
    }
}

}
