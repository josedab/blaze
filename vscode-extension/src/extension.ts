import * as vscode from 'vscode';
import { LanguageClient, LanguageClientOptions, ServerOptions } from 'vscode-languageclient/node';

let client: LanguageClient | undefined;

export function activate(context: vscode.ExtensionContext) {
    const config = vscode.workspace.getConfiguration('blaze-sql');
    const serverPath = config.get<string>('serverPath', 'blaze');
    const serverArgs = config.get<string[]>('serverArgs', ['--lsp']);

    const serverOptions: ServerOptions = {
        command: serverPath,
        args: serverArgs,
    };

    const clientOptions: LanguageClientOptions = {
        documentSelector: [
            { scheme: 'file', language: 'sql' },
            { scheme: 'file', language: 'blaze-sql' },
        ],
        synchronize: {
            fileEvents: vscode.workspace.createFileSystemWatcher('**/*.sql'),
        },
    };

    client = new LanguageClient(
        'blaze-sql',
        'Blaze SQL Language Server',
        serverOptions,
        clientOptions
    );

    client.start();
    context.subscriptions.push({
        dispose: () => client?.stop(),
    });
}

export function deactivate(): Thenable<void> | undefined {
    return client?.stop();
}
