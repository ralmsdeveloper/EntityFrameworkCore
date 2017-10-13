// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Data.Common;
using System.Reflection;
using JetBrains.Annotations;

namespace Microsoft.EntityFrameworkCore.Storage
{
    /// <summary>
    ///     <para>
    ///         Allows database providers to choose specific methods for reading a value from a <see cref="DbDataReader" />.
    ///     </para>
    ///     <para>
    ///         Providers that need this should typically derive from <see cref="DefaultDataReaderMethodProvider"/>.
    ///     </para>
    ///     <para>
    ///         This type is typically used by database providers (and other extensions). It is generally
    ///         not used in application code.
    ///     </para>
    /// </summary>
    public interface IDataReaderMethodProvider
    {
        /// <summary>
        ///     The method to use when reading values of the given type. The method must be defined
        ///     on <see cref="DbDataReader" /> or one of its subclasses.
        /// </summary>
        /// <param name="type"> The type of the value to be read. </param>
        /// <returns> The method to use to read the value. </returns>
        MethodInfo GetReadMethod([NotNull] Type type);
    }
}
