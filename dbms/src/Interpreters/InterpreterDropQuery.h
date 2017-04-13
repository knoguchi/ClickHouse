#pragma once

#include <Storages/IStorage.h>
#include <Interpreters/Context.h>
#include <Interpreters/IInterpreter.h>


namespace DB
{


/** Allows to delete a table along with all the data (DROP), or to delete info about the table from the server (DETACH).
  */
class InterpreterDropQuery : public IInterpreter
{
public:
    InterpreterDropQuery(ASTPtr query_ptr_, Context & context_);

    /// Deletes table or database.
    BlockIO execute() override;

private:
    ASTPtr query_ptr;
    Context context;

    BlockIO executeOnServer(ASTDropQuery & drop);
    BlockIO executeOnCluster(ASTDropQuery & drop);
};


}
