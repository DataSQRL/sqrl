/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const fs = require('fs');
const path = require('path');
const yaml = require('js-yaml');

// File paths
const SYSTEM_FUNCTIONS_YAML = path.join(__dirname, '../docs/stdlib-docs/stdlib-docs/system-functions.yml');
const LIBRARY_FUNCTIONS_YAML = path.join(__dirname, '../docs/stdlib-docs/stdlib-docs/library-functions.yml');
const SYSTEM_FUNCTIONS_OUTPUT = path.join(__dirname, '../docs/functions-system-generated.md');
const LIBRARY_FUNCTIONS_OUTPUT = path.join(__dirname, '../docs/functions-library-generated.md');

/**
 * Generates a markdown table row for a function
 */
function generateSystemFunctionRow(func) {
    const name = func.name || '';
    const description = func.description || '';
    const example = func.example || '';
    
    return `| \`${name}\` | ${description} | \`${example}\` |`;
}

function generateLibraryFunctionRow(func, categoryName) {
    const name = func.name || '';
    const description = func.description || '';
    const identifier = func.identifier || '';
    const requirement = func.requirement || '';
    const importStatement = `IMPORT ${categoryName}.${identifier};`;
    
    return `| \`${name}\` | ${description} | \`${importStatement}\` | ${requirement} |`;
}

/**
 * Generates a markdown section for a category of functions
 */
function generateSystemSection(categoryName, functions) {
    const header = `## ${categoryName.charAt(0).toUpperCase() + categoryName.slice(1)} Functions

| Function | Description | Example |
|----------|-------------|---------|
`;
    
    const rows = functions.map(generateSystemFunctionRow).join('\n');
    
    return header + rows + '\n';
}

function generateLibrarySection(categoryName, functions) {
    const header = `## ${categoryName.charAt(0).toUpperCase() + categoryName.slice(1)} Functions

| Function | Description | Import     | Requirements |
|----------|-------------|------------|--------------|
`;
    
    const rows = functions.map(func => generateLibraryFunctionRow(func, categoryName)).join('\n');
    
    return header + rows + '\n';
}

/**
 * Generates the complete system functions markdown file
 */
function generateSystemFunctionsDoc(data) {
    const frontmatter = `# System Functions

DataSQRL provides built-in system functions that are available in all SQRL scripts. These functions are grouped by functionality and provide essential operations for data processing, JSON manipulation, vector operations, and text processing.

`;
    
    const sections = Object.entries(data)
        .map(([category, functions]) => generateSystemSection(category, functions))
        .join('\n');
    
    return frontmatter + sections;
}

/**
 * Generates the complete library functions markdown file
 */
function generateLibraryFunctionsDoc(data) {
    const frontmatter = `# Library Functions

DataSQRL provides extended library functions that can be imported into your SQRL scripts. These functions offer specialized capabilities for mathematical operations, data processing, AI integration, and external system connectivity.

Library functions must be imported into the SQRL script via one of the following \`IMPORT\` statements:

\`\`\`sql
IMPORT stdlib.library-name.*; --imports all functions in the library
IMPORT stdlib.library-name.function-name; --imports a single function by name
IMPORT stdlib.library-name.function-name AS myName; --imports a single function under a given name
\`\`\`
`;
    
    const sections = Object.entries(data)
        .map(([category, functions]) => generateLibrarySection(category, functions))
        .join('\n');
    
    return frontmatter + sections;
}

/**
 * Main generator function
 */
function generateFunctionDocs() {
    try {
        console.log('🔧 Generating function documentation...');
        
        // Read and parse YAML files
        console.log('📖 Reading YAML files...');
        const systemFunctionsData = yaml.load(fs.readFileSync(SYSTEM_FUNCTIONS_YAML, 'utf8'));
        const libraryFunctionsData = yaml.load(fs.readFileSync(LIBRARY_FUNCTIONS_YAML, 'utf8'));
        
        // Generate markdown content
        console.log('📝 Generating markdown content...');
        const systemFunctionsMarkdown = generateSystemFunctionsDoc(systemFunctionsData);
        const libraryFunctionsMarkdown = generateLibraryFunctionsDoc(libraryFunctionsData);
        
        // Write output files
        console.log('💾 Writing output files...');
        fs.writeFileSync(SYSTEM_FUNCTIONS_OUTPUT, systemFunctionsMarkdown);
        fs.writeFileSync(LIBRARY_FUNCTIONS_OUTPUT, libraryFunctionsMarkdown);
        
        console.log('✅ Function documentation generated successfully!');
        console.log(`   - ${SYSTEM_FUNCTIONS_OUTPUT}`);
        console.log(`   - ${LIBRARY_FUNCTIONS_OUTPUT}`);
        
    } catch (error) {
        console.error('❌ Error generating function documentation:', error);
        process.exit(1);
    }
}

// Run the generator if called directly
if (require.main === module) {
    generateFunctionDocs();
}

module.exports = { generateFunctionDocs };
