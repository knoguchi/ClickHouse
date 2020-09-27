#include <Parsers/ASTFrameElement.h>
#include <Common/SipHash.h>


namespace DB
{

ASTPtr ASTFrameElement::clone() const
{
    auto clone = std::make_shared<ASTFrameElement>(*this);
    clone->cloneChildren();
    return clone;
}

void ASTFrameElement::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (unbounded_preceding)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " UNBOUNDED PRECEDING" << (settings.hilite ? hilite_keyword : "");
    else if (current_row)
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " CURRENT ROW" << (settings.hilite ? hilite_keyword : "");
    else
    {
        settings.ostr << ' ';
        value->formatImpl(settings, state, frame);
        if (preceding)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " PRECEDING" << (settings.hilite ? hilite_keyword : "");
        else if (following)
            settings.ostr << (settings.hilite ? hilite_keyword : "") << " FOLLOWING" << (settings.hilite ? hilite_keyword : "");
    }
}

}
